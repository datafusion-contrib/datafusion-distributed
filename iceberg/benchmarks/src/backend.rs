use std::path::Path;

use async_trait::async_trait;
use datafusion::config::{ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_benchmarks::backend::BenchmarkBackend;
use datafusion_distributed_iceberg::{IcebergConfig, IcebergExt, IcebergIntegrationOptions};

use crate::dataset;

/// Iceberg registration and session extensions for local and remote benchmarks.
#[derive(Clone)]
pub struct IcebergBenchmarkBackend;

#[async_trait]
impl BenchmarkBackend for IcebergBenchmarkBackend {
    async fn register_tables(&mut self, ctx: &SessionContext, path: &Path) -> Result<()> {
        dataset::register_tables(ctx, path).await
    }

    fn configure_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder {
        let mut builder = builder
            .with_iceberg_integration(IcebergIntegrationOptions::default())
            .with_iceberg_column_stats_enabled(true);
        if let Some(config) = builder.config().as_mut() {
            apply_iceberg_env(config.options_mut()).unwrap_or_else(|error| panic!("{error}"));
        }
        builder
    }
}

/// Sets `iceberg.*` options from environment variables. It works the same way DataFusion reads
/// `DATAFUSION_*` variables. Each option has one variable: its name in uppercase, with dots
/// replaced by underscores. For example, `ICEBERG_ROW_SELECTION_ENABLED=true` sets
/// `iceberg.row_selection_enabled`. Variables with other names are ignored. An invalid value
/// returns an error.
fn apply_iceberg_env(options: &mut ConfigOptions) -> Result<()> {
    for entry in IcebergConfig::default().entries() {
        let key = format!("{}.{}", IcebergConfig::PREFIX, entry.key);
        if let Some(value) = std::env::var_os(key.to_uppercase().replace('.', "_")) {
            options.set(&key, value.to_string_lossy().as_ref())?;
        }
    }
    Ok(())
}
