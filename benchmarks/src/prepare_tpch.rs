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
use datafusion_distributed_benchmarks::datasets::output::TESTDATA_ROOT;
use datafusion_distributed_benchmarks::datasets::parquet_layout::ParquetLayout;
use datafusion_distributed_benchmarks::datasets::tpch::{TpchSpec, generate_data};
use structopt::StructOpt;

/// Generate TPC-H parquet files for benchmarks
#[derive(Debug, StructOpt)]
pub struct PrepareTpchOpt {
    /// Local directory or s3://bucket/prefix under which the dataset path is created
    #[structopt(long, default_value = TESTDATA_ROOT.as_str())]
    root: String,

    /// Scale factor (e.g. 1, 10, 100)
    #[structopt(short = "s", long = "scale-factor", default_value = "1")]
    scale_factor: f64,

    #[structopt(flatten)]
    layout: ParquetLayout,
}

impl PrepareTpchOpt {
    pub async fn run(self) -> Result<()> {
        let spec = TpchSpec {
            location: self.root,
            scale_factor: self.scale_factor,
            layout: self.layout,
        };
        generate_data(&spec)
            .await
            .map_err(|e| exec_datafusion_err!("{e}"))?;
        // The only output on stdout, so that `DATASET=$(dfbench prepare-...)` captures it.
        println!("{}", spec.dataset());
        Ok(())
    }
}
