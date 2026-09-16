//! Local Iceberg benchmark preparation and execution.

use datafusion::error::Result;
use datafusion_distributed_benchmarks::run::RunOpt;
use datafusion_distributed_iceberg_benchmarks::{
    backend::IcebergBenchmarkBackend, prepare::PrepareIcebergOpt,
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "Iceberg benchmark command")]
enum Options {
    /// Convert a local Parquet dataset to Iceberg.
    Prepare(PrepareIcebergOpt),
    /// Run benchmarks against a prepared Iceberg dataset.
    Run(RunOpt),
}

fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Prepare(options) => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(options.run())
        }
        Options::Run(options) => options.run(IcebergBenchmarkBackend),
    }
}
