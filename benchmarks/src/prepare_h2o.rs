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
use datafusion_distributed_benchmarks::datasets::h2o::{generate_h2o_data, rows_for_scale_factor};
use std::path::PathBuf;
use structopt::StructOpt;

/// Generate h2o groupby parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareH2oOpt {
    /// Output path for generated parquet files
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Scale factor. SF1 is 10 million rows (official "small").
    #[structopt(short = "s", long = "scale-factor", default_value = "1")]
    scale_factor: f64,

    /// Number of partitions (parquet files)
    #[structopt(short = "n", long = "partitions", default_value = "16")]
    partitions: usize,
}

impl PrepareH2oOpt {
    pub fn run(self) -> Result<()> {
        let n_rows = rows_for_scale_factor(self.scale_factor)?;
        println!(
            "Generating h2o groupby data ({} rows, {} partitions) in '{}'",
            n_rows,
            self.partitions,
            self.output_path.display()
        );
        generate_h2o_data(&self.output_path, n_rows, self.partitions)
            .map_err(|e| datafusion::error::DataFusionError::Internal(format!("{e:?}")))?;
        println!("h2o data generation complete.");
        Ok(())
    }
}
