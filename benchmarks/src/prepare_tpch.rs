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

use datafusion::common::{Result, exec_datafusion_err};
use datafusion_distributed_benchmarks::datasets::{output::DatasetOutput, tpch::generate_data};
use structopt::StructOpt;

/// Generate TPC-H parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareTpchOpt {
    /// Empty local directory or s3://bucket/prefix for generated Parquet files
    #[structopt(required = true, short = "o", long = "output")]
    output_path: String,

    /// Scale factor (e.g. 1.0, 10.0, 100.0)
    #[structopt(short = "s", long = "scale-factor", default_value = "1")]
    scale_factor: f64,

    /// Number of partitions (parquet files per table)
    #[structopt(short = "n", long = "partitions", default_value = "16")]
    partitions: usize,

    /// Write Parquet `sorting_columns` metadata for the order tpchgen already emits
    #[structopt(long)]
    sorted: bool,
}

impl PrepareTpchOpt {
    pub async fn run(self) -> Result<()> {
        let label = if self.sorted { "sorted TPC-H" } else { "TPC-H" };
        println!(
            "Generating {label} data at scale factor {} with {} partitions in '{}'",
            self.scale_factor, self.partitions, self.output_path
        );
        let output = DatasetOutput::new(&self.output_path).await?;
        generate_data(&output, self.scale_factor, self.partitions, self.sorted)
            .await
            .map_err(|e| exec_datafusion_err!("{e}"))?;
        println!("{label} data generation complete.");
        Ok(())
    }
}
