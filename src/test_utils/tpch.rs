use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    catalog::{MemTable, TableProvider},
};

use std::fs;

use arrow::record_batch::RecordBatch;
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow, RegionArrow,
    SupplierArrow,
};

pub fn tpch_query_from_dir(queries_dir: &std::path::Path, num: u8) -> String {
    let query_path = queries_dir.join(format!("q{num}.sql"));
    fs::read_to_string(query_path)
        .unwrap_or_else(|_| panic!("Failed to read TPCH query file: q{num}.sql"))
        .trim()
        .to_string()
}
pub const NUM_QUERIES: u8 = 22; // number of queries in the TPCH benchmark numbered from 1 to 22

pub fn tpch_table(name: &str) -> Arc<dyn TableProvider> {
    let schema = Arc::new(get_tpch_table_schema(name));
    Arc::new(MemTable::try_new(schema, vec![]).unwrap())
}

pub fn get_tpch_table_schema(table: &str) -> Schema {
    // note that the schema intentionally uses signed integers so that any generated Parquet
    // files can also be used to benchmark tools that only support signed integers, such as
    // Apache Spark

    match table {
        "part" => Schema::new(vec![
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

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
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

        "lineitem" => Schema::new(vec![
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

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

// generate_table creates a parquet file in the data directory from an arrow RecordBatch row
// source.
fn generate_table<A>(
    mut data_source: A,
    table_name: &str,
    data_dir: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>>
where
    A: Iterator<Item = RecordBatch>,
{
    let output_path = data_dir.join(format!("{table_name}.parquet"));

    if let Some(first_batch) = data_source.next() {
        let file = fs::File::create(&output_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, first_batch.schema(), Some(props))?;

        writer.write(&first_batch)?;

        for batch in data_source {
            writer.write(&batch)?;
        }

        writer.close()?;
    }

    Ok(())
}

// generate_tpch_data generates all TPC-H tables in the specified data directory.
pub fn generate_tpch_data(data_dir: &std::path::Path, sf: f64, parts: i32) {
    fs::create_dir_all(data_dir).expect("Failed to create data directory");

    macro_rules! must_generate_tpch_table {
        ($generator:ident, $arrow:ident, $name:literal) => {
            let data_dir = data_dir.join($name);
            fs::create_dir_all(data_dir.clone()).expect("Failed to create data directory");
            // create three partitions for the table
            (1..=parts).for_each(|part| {
                generate_table(
                    // TODO: Consider adjusting the partitions and batch sizes.
                    $arrow::new($generator::new(sf, part, parts)).with_batch_size(1000),
                    &format!("{part}"),
                    &data_dir,
                )
                .expect(concat!("Failed to generate ", $name, " table"));
            });
        };
    }

    must_generate_tpch_table!(RegionGenerator, RegionArrow, "region");
    must_generate_tpch_table!(NationGenerator, NationArrow, "nation");
    must_generate_tpch_table!(CustomerGenerator, CustomerArrow, "customer");
    must_generate_tpch_table!(SupplierGenerator, SupplierArrow, "supplier");
    must_generate_tpch_table!(PartGenerator, PartArrow, "part");
    must_generate_tpch_table!(PartSuppGenerator, PartSuppArrow, "partsupp");
    must_generate_tpch_table!(OrderGenerator, OrderArrow, "orders");
    must_generate_tpch_table!(LineItemGenerator, LineItemArrow, "lineitem");
}
