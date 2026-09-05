use std::sync::Arc;

use datafusion::common::extensions_options;
use datafusion::config::{ConfigExtension, ConfigOptions};
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;

extensions_options! {
    /// Configuration for Iceberg table reads.
    pub struct IcebergConfig {
        /// Maximum number of data files to read concurrently. Must be greater than zero.
        pub data_file_concurrency_limit: usize, default = 8
        /// Whether to prune Parquet row groups using their statistics.
        pub row_group_filtering_enabled: bool, default = true
        /// Whether to apply row-level selections while reading Parquet files.
        pub row_selection_enabled: bool, default = false
        /// Whether to include column statistics read during planning
        pub column_stats_enabled: bool, default = false
    }
}

impl ConfigExtension for IcebergConfig {
    const PREFIX: &'static str = "iceberg";
}

impl IcebergConfig {
    /// Returns the registered Iceberg configuration, or its defaults when it
    /// has not been registered.
    ///
    /// Register [`IcebergConfig::default`] through
    /// [`SessionConfig::with_option_extension`] to configure these settings
    /// with `iceberg.*` DataFusion options.
    pub fn from_config_options(cfg: &ConfigOptions) -> Self {
        cfg.extensions.get::<Self>().cloned().unwrap_or_default()
    }

    /// Returns the registered Iceberg configuration, or its defaults.
    pub fn from_session_config(session_cfg: &SessionConfig) -> Self {
        Self::from_config_options(session_cfg.options())
    }

    /// Returns the registered Iceberg configuration, or its defaults.
    pub fn from_task_context(ctx: &Arc<TaskContext>) -> Self {
        Self::from_session_config(ctx.session_config())
    }
}

#[cfg(test)]
mod tests {
    use super::IcebergConfig;
    use datafusion::prelude::SessionConfig;

    #[test]
    fn config_options_can_be_set_through_session_config() {
        let config = SessionConfig::new()
            .with_option_extension(IcebergConfig::default())
            .set_usize("iceberg.data_file_concurrency_limit", 8)
            .set_bool("iceberg.row_group_filtering_enabled", false)
            .set_bool("iceberg.row_selection_enabled", true);

        let iceberg_config = IcebergConfig::from_session_config(&config);
        assert_eq!(iceberg_config.data_file_concurrency_limit, 8);
        assert!(!iceberg_config.row_group_filtering_enabled);
        assert!(iceberg_config.row_selection_enabled);
    }

    #[test]
    fn unregistered_config_uses_defaults() {
        assert_eq!(
            IcebergConfig::from_session_config(&SessionConfig::new()).data_file_concurrency_limit,
            IcebergConfig::default().data_file_concurrency_limit,
        );
    }
}
