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

use datafusion::error::Result;
use datafusion_distributed_benchmarks::datasets::tpch::generate_tpch_data;
use std::path::PathBuf;
use structopt::StructOpt;

/// Generate TPC-H parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct GenerateTpchOpt {
    /// Output path for generated parquet files
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Scale factor (e.g. 1.0, 10.0, 100.0)
    #[structopt(short = "s", long = "scale-factor", default_value = "1")]
    scale_factor: f64,

    /// Number of partitions (parquet files per table)
    #[structopt(short = "n", long = "partitions", default_value = "16")]
    partitions: usize,
}

impl GenerateTpchOpt {
    pub fn run(self) -> Result<()> {
        println!(
            "Generating TPC-H data at scale factor {} with {} partitions in '{}'",
            self.scale_factor,
            self.partitions,
            self.output_path.display()
        );
        generate_tpch_data(&self.output_path, self.scale_factor, self.partitions)
            .map_err(|e| datafusion::error::DataFusionError::Internal(format!("{e:?}")))?;
        println!("TPC-H data generation complete.");
        Ok(())
    }
}
