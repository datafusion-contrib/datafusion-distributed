use std::path::Path;

use async_trait::async_trait;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_benchmarks::backend::BenchmarkBackend;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};

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
            apply_iceberg_env(config.options_mut(), std::env::vars())
                .unwrap_or_else(|error| panic!("{error}"));
        }
        builder
    }
}

/// Sets `iceberg.*` options from environment variables, as `SessionConfig::from_env` does for
/// `DATAFUSION_*`: the variable is the option name in uppercase with the dots replaced by
/// underscores, so `ICEBERG_ROW_SELECTION_ENABLED=true` sets `iceberg.row_selection_enabled`.
/// It works for every option the Iceberg integration registers. A name or value the integration
/// rejects is an error, so a misspelled variable cannot silently leave a benchmark unconfigured.
fn apply_iceberg_env(
    options: &mut ConfigOptions,
    vars: impl Iterator<Item = (String, String)>,
) -> Result<()> {
    for (name, value) in vars {
        if let Some(option) = name.strip_prefix("ICEBERG_") {
            options.set(&format!("iceberg.{}", option.to_lowercase()), &value)?;
        }
    }
    Ok(())
}
