mod dataset;
pub mod prepare;

use std::path::Path;

use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_benchmarks::backend::BenchmarkBackend;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};

/// Iceberg registration and session extensions for local and remote benchmarks.
pub struct IcebergBenchmarkBackend;

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
