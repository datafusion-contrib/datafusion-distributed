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

use datafusion::error::{DataFusionError, Result};
use datafusion_distributed_benchmarks::datasets::imdb::{IMDB_TABLES, convert_imdb_csv_to_parquet};
use std::path::PathBuf;
use structopt::StructOpt;

/// Convert Join Order Benchmark IMDB CSV files to parquet
#[derive(Debug, StructOpt)]
pub struct PrepareImdbOpt {
    /// Directory containing `<table>.csv` files extracted from imdb.tgz
    #[structopt(parse(from_os_str), required = true, short = "i", long = "input")]
    input_path: PathBuf,

    /// Output path. Each table is written to `<output>/<table>/0.parquet`
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,
}

impl PrepareImdbOpt {
    pub async fn run(self) -> Result<()> {
        println!(
            "Converting IMDB CSVs from '{}' to parquet under '{}'",
            self.input_path.display(),
            self.output_path.display()
        );
        convert_imdb_csv_to_parquet(&self.input_path, &self.output_path, IMDB_TABLES)
            .await
            .map_err(|e| DataFusionError::Internal(format!("{e:?}")))?;
        println!("IMDB conversion complete.");
        Ok(())
    }
}
