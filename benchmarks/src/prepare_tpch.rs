// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::instant::Instant;
use datafusion::error::Result;
use datafusion::logical_expr::select_expr::SelectExpr;
use datafusion::parquet::basic::Compression;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use structopt::StructOpt;

/// Prepare TPCH parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareTpchOpt {
    /// Path to unprepared files
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Number of partitions to produce. By default, uses only 1 partition.
    #[structopt(short = "n", long = "partitions", default_value = "1")]
    partitions: usize,

    /// Sort each table by its first column in ascending order.
    #[structopt(short = "t", long = "sort")]
    sort: bool,
}

impl PrepareTpchOpt {
    pub async fn run(self) -> Result<()> {
        let input_path = self.input_path.to_str().unwrap();
        let output_path = self.output_path.to_str().unwrap();

        let output_root_path = Path::new(output_path);
        for table in TPCH_TABLES {
            let start = Instant::now();
            let schema = get_tpch_table_schema(table);
            let key_column_name = schema.fields()[0].name();

            let input_path = format!("{input_path}/{table}.tbl");
            let options = CsvReadOptions::new()
                .schema(&schema)
                .has_header(false)
                .delimiter(b'|')
                .file_extension(".tbl");
            let options = if self.sort {
                // indicated that the file is already sorted by its first column to speed up the conversion
                options.file_sort_order(vec![vec![col(key_column_name).sort(true, false)]])
            } else {
                options
            };

            let config = SessionConfig::new().with_target_partitions(self.partitions);
            let ctx = SessionContext::new_with_config(config);

            // build plan to read the TBL file
            let mut csv = ctx.read_csv(&input_path, options).await?;

            // Select all apart from the padding column
            let selection = csv
                .schema()
                .iter()
                .take(schema.fields.len() - 1)
                .map(Expr::from)
                .map(SelectExpr::from)
                .collect::<Vec<_>>();

            csv = csv.select(selection)?;
            csv = csv.repartition(Partitioning::RoundRobinBatch(self.partitions))?;
            let csv = if self.sort {
                csv.sort_by(vec![col(key_column_name)])?
            } else {
                csv
            };

            // create the physical plan
            let csv = csv.create_physical_plan().await?;

            let output_path = output_root_path.join(table);
            let output_path = output_path.to_str().unwrap().to_owned();
            fs::create_dir_all(&output_path)?;
            println!(
                "Converting '{}' to parquet files in directory '{}'",
                &input_path, &output_path
            );
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .build();
            ctx.write_parquet(csv, output_path, Some(props)).await?;
            println!("Conversion completed in {} ms", start.elapsed().as_millis());
        }

        Ok(())
    }
}

const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

pub fn get_tpch_table_schema(table: &str) -> datafusion::arrow::datatypes::Schema {
    // note that the schema intentionally uses signed integers so that any generated Parquet
    // files can also be used to benchmark tools that only support signed integers, such as
    // Apache Spark

    match table {
        "part" => datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => datafusion::arrow::datatypes::Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}
