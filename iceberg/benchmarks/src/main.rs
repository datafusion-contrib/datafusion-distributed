//! Local Iceberg benchmark preparation and execution.
mod dataset;
mod prepare;

use std::path::Path;

use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
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

struct IcebergBenchmarkBackend;

#[async_trait]
impl BenchmarkBackend for IcebergBenchmarkBackend {
    async fn register_tables(&mut self, ctx: &SessionContext, path: &Path) -> Result<()> {
        dataset::register_tables(ctx, path).await
    }

    fn configure_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder {
        builder
            .with_iceberg_integration(IcebergIntegrationOptions::default())
            .with_iceberg_column_stats_enabled(true)
    }
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
