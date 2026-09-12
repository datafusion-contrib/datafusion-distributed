//! Local benchmark dataset preparation, registration, and session configuration.
//! Enabled only by the `benchmarks` feature; this is not a general-purpose write API.

mod dataset;
mod prepare;

use datafusion::execution::SessionStateBuilder;

use crate::{IcebergExt, IcebergIntegrationOptions};

pub use dataset::register_tables;
pub use prepare::PrepareIcebergOpt;

/// Install the Iceberg benchmark configuration, including manifest column statistics.
pub fn configure_session(builder: SessionStateBuilder) -> SessionStateBuilder {
    builder
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .with_iceberg_column_stats_enabled(true)
}

#[cfg(test)]
mod tests {
    use datafusion::execution::SessionStateBuilder;

    use crate::IcebergConfig;

    #[tokio::test]
    async fn benchmark_sessions_load_column_statistics() {
        let state = super::configure_session(SessionStateBuilder::new()).build();
        assert!(IcebergConfig::from_session_config(state.config()).column_stats_enabled);
    }
}
