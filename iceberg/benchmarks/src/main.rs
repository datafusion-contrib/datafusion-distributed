//! Local Iceberg benchmark preparation and execution.
mod dataset;
mod prepare;

use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed_benchmarks::backend::BenchmarkBackend;
use datafusion_distributed_benchmarks::run::RunOpt;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "Iceberg benchmark command")]
enum Options {
    /// Convert a local Parquet dataset to Iceberg.
    Prepare(prepare::PrepareIcebergOpt),
    /// Run benchmarks against a prepared Iceberg dataset.
    Run(RunOpt),
}

fn configure_session(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .with_iceberg_column_stats_enabled(true)
}

fn iceberg_backend() -> BenchmarkBackend {
    BenchmarkBackend::new(
        |ctx, path| Box::pin(dataset::register_tables(ctx, path)),
        configure_session,
    )
}

fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Prepare(options) => {
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(options.run())
        }
        Options::Run(options) => options.run(iceberg_backend()),
    }
}
