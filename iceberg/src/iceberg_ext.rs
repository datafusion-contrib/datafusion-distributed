use std::sync::Arc;

use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::DistributedExt;
use delegate::delegate;
use iceberg::io::StorageFactory;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

use crate::codec::IcebergCodec;
use crate::distributed_desired_task_count_handler::iceberg_desired_task_count;
use crate::{IcebergConfig, IcebergDataSource, IcebergTableProviderFactory};

/// Configuration required to register the Iceberg SQL integration.
pub struct IcebergIntegrationOptions {
    /// Builds storage implementations for table metadata and data files.
    pub storage_factory: Arc<dyn StorageFactory>,
    /// Executes Iceberg IO-bound and CPU-bound work.
    ///
    /// Construct this from the application's Tokio runtime with
    /// [`iceberg::Runtime::new`] or [`iceberg::Runtime::new_with_split`].
    pub iceberg_runtime: iceberg::Runtime,
}

impl Default for IcebergIntegrationOptions {
    fn default() -> Self {
        Self {
            storage_factory: Arc::new(OpenDalResolvingStorageFactory::new()),
            iceberg_runtime: iceberg::Runtime::current(),
        }
    }
}

/// Extends DataFusion session types with the Iceberg integration and its
/// configuration.
pub trait IcebergExt: Sized {
    /// Registers the `ICEBERG` table provider factory and the default Iceberg
    /// configuration.
    fn set_iceberg_integration(&mut self, options: IcebergIntegrationOptions);

    /// Registers the `ICEBERG` table provider factory and the default Iceberg
    /// configuration.
    fn with_iceberg_integration(self, options: IcebergIntegrationOptions) -> Self;

    /// Sets the maximum number of Iceberg data files read concurrently.
    fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize);

    /// Sets the maximum number of Iceberg data files read concurrently.
    fn with_iceberg_data_file_concurrency_limit(self, limit: usize) -> Self;

    /// Enables or disables Parquet row-group filtering for Iceberg reads.
    fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool);

    /// Enables or disables Parquet row-group filtering for Iceberg reads.
    fn with_iceberg_row_group_filtering_enabled(self, enabled: bool) -> Self;

    /// Enables or disables row-level selection for Iceberg reads.
    fn set_iceberg_row_selection_enabled(&mut self, enabled: bool);

    /// Enables or disables row-level selection for Iceberg reads.
    fn with_iceberg_row_selection_enabled(self, enabled: bool) -> Self;
}

trait IcebergConfigExt {
    fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize);
    fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool);
    fn set_iceberg_row_selection_enabled(&mut self, enabled: bool);
}

impl IcebergConfigExt for SessionConfig {
    fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize) {
        iceberg_config_mut(self).data_file_concurrency_limit = limit;
    }

    fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool) {
        iceberg_config_mut(self).row_group_filtering_enabled = enabled;
    }

    fn set_iceberg_row_selection_enabled(&mut self, enabled: bool) {
        iceberg_config_mut(self).row_selection_enabled = enabled;
    }
}

fn iceberg_config_mut(config: &mut SessionConfig) -> &mut IcebergConfig {
    if config.options().extensions.get::<IcebergConfig>().is_none() {
        config
            .options_mut()
            .extensions
            .insert(IcebergConfig::default());
    }

    config
        .options_mut()
        .extensions
        .get_mut::<IcebergConfig>()
        .expect("IcebergConfig was inserted above")
}

const TABLE_FACTORY_IDENTIFIER: &str = "ICEBERG";

fn set_iceberg_integration(state: &mut SessionState, options: IcebergIntegrationOptions) {
    iceberg_config_mut(state.config_mut());
    let codec = IcebergCodec::new(
        Arc::clone(&options.storage_factory),
        options.iceberg_runtime.clone(),
    );
    state.table_factories_mut().insert(
        TABLE_FACTORY_IDENTIFIER.to_string(),
        Arc::new(IcebergTableProviderFactory::new_with_runtime(
            options.storage_factory,
            options.iceberg_runtime,
        )),
    );
    state.set_distributed_desired_task_count_handler(iceberg_desired_task_count);
    state.set_distributed_work_unit_feed(|plan: &DataSourceExec| {
        plan.data_source()
            .downcast_ref::<IcebergDataSource>()
            .map(IcebergDataSource::feed)
    });
    state.set_distributed_user_codec(codec);
}

impl IcebergExt for SessionStateBuilder {
    fn set_iceberg_integration(&mut self, options: IcebergIntegrationOptions) {
        iceberg_config_mut(self.config().get_or_insert_default());
        let codec = IcebergCodec::new(
            Arc::clone(&options.storage_factory),
            options.iceberg_runtime.clone(),
        );
        self.table_factories().get_or_insert_default().insert(
            TABLE_FACTORY_IDENTIFIER.to_string(),
            Arc::new(IcebergTableProviderFactory::new_with_runtime(
                options.storage_factory,
                options.iceberg_runtime,
            )),
        );
        self.set_distributed_desired_task_count_handler(iceberg_desired_task_count);
        self.set_distributed_work_unit_feed(|plan: &DataSourceExec| {
            plan.data_source()
                .downcast_ref::<IcebergDataSource>()
                .map(IcebergDataSource::feed)
        });
        self.set_distributed_user_codec(codec);
    }

    delegate! {
        to self.config().get_or_insert_default() {
            fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize);
            fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool);
            fn set_iceberg_row_selection_enabled(&mut self, enabled: bool);
        }

        to self {
            #[call(set_iceberg_integration)]
            #[expr($;self)]
            fn with_iceberg_integration(mut self, options: IcebergIntegrationOptions) -> Self;

            #[call(set_iceberg_data_file_concurrency_limit)]
            #[expr($;self)]
            fn with_iceberg_data_file_concurrency_limit(mut self, limit: usize) -> Self;

            #[call(set_iceberg_row_group_filtering_enabled)]
            #[expr($;self)]
            fn with_iceberg_row_group_filtering_enabled(mut self, enabled: bool) -> Self;

            #[call(set_iceberg_row_selection_enabled)]
            #[expr($;self)]
            fn with_iceberg_row_selection_enabled(mut self, enabled: bool) -> Self;
        }
    }
}

impl IcebergExt for SessionState {
    fn set_iceberg_integration(&mut self, options: IcebergIntegrationOptions) {
        set_iceberg_integration(self, options);
    }

    delegate! {
        to self.config_mut() {
            fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize);
            fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool);
            fn set_iceberg_row_selection_enabled(&mut self, enabled: bool);
        }

        to self {
            #[call(set_iceberg_integration)]
            #[expr($;self)]
            fn with_iceberg_integration(mut self, options: IcebergIntegrationOptions) -> Self;

            #[call(set_iceberg_data_file_concurrency_limit)]
            #[expr($;self)]
            fn with_iceberg_data_file_concurrency_limit(mut self, limit: usize) -> Self;

            #[call(set_iceberg_row_group_filtering_enabled)]
            #[expr($;self)]
            fn with_iceberg_row_group_filtering_enabled(mut self, enabled: bool) -> Self;

            #[call(set_iceberg_row_selection_enabled)]
            #[expr($;self)]
            fn with_iceberg_row_selection_enabled(mut self, enabled: bool) -> Self;
        }
    }
}

impl IcebergExt for SessionContext {
    delegate! {
        to self.state_ref().write() {
            fn set_iceberg_integration(&mut self, options: IcebergIntegrationOptions);
            fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize);
            fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool);
            fn set_iceberg_row_selection_enabled(&mut self, enabled: bool);
        }

        to self {
            #[call(set_iceberg_integration)]
            #[expr($;self)]
            fn with_iceberg_integration(mut self, options: IcebergIntegrationOptions) -> Self;

            #[call(set_iceberg_data_file_concurrency_limit)]
            #[expr($;self)]
            fn with_iceberg_data_file_concurrency_limit(mut self, limit: usize) -> Self;

            #[call(set_iceberg_row_group_filtering_enabled)]
            #[expr($;self)]
            fn with_iceberg_row_group_filtering_enabled(mut self, enabled: bool) -> Self;

            #[call(set_iceberg_row_selection_enabled)]
            #[expr($;self)]
            fn with_iceberg_row_selection_enabled(mut self, enabled: bool) -> Self;
        }
    }
}
