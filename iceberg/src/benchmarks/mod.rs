//! Local benchmark dataset preparation, registration, and session configuration.
//! Enabled only by the `benchmarks` feature; this is not a general-purpose write API.

mod dataset;
mod prepare;

use datafusion::execution::SessionStateBuilder;
use structopt::StructOpt;

use crate::{IcebergExt, IcebergIntegrationOptions};

pub use dataset::register_tables;
pub use prepare::PrepareIcebergOpt;

/// Iceberg-specific options shared by benchmark coordinator and worker sessions.
#[derive(Debug, Default, StructOpt)]
pub struct IcebergBenchmarkOptions {
    /// Load Iceberg manifest column statistics during planning.
    #[structopt(long = "iceberg-column-stats")]
    column_stats: bool,
}

impl IcebergBenchmarkOptions {
    /// Install Iceberg support on a session builder within an active Tokio runtime.
    pub fn configure_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder {
        builder
            .with_iceberg_integration(IcebergIntegrationOptions::default())
            .with_iceberg_column_stats_enabled(self.column_stats)
    }
}
