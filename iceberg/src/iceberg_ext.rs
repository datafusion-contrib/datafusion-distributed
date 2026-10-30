use std::sync::Arc;

use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_distributed::DistributedExt;
use delegate::delegate;
use iceberg::io::StorageFactory;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;

use crate::distributed_desired_task_count_handler::iceberg_desired_task_count;
use crate::distributed_scale_up_handler::iceberg_scale_up;
use crate::{EagerIcebergCodec, IcebergFileIOCodec, LazyIcebergCodec};
use crate::{IcebergConfig, IcebergTableProviderFactory, LazyIcebergDataSource};

/// Configuration required to register the Iceberg SQL integration.
#[derive(Clone)]
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

    fn set_iceberg_column_stats_enabled(&mut self, enabled: bool);
    fn with_iceberg_column_stats_enabled(self, enabled: bool) -> Self;
}

/// Registers Iceberg sessions with application-defined file IO serialization.
pub trait IcebergFileIOExt: Sized {
    /// Registers the Iceberg integration with custom file IO serialization.
    /// Use this instead of `set_iceberg_integration` when installing a file IO hook.
    fn set_iceberg_integration_with_file_io_codec(
        &mut self,
        options: IcebergIntegrationOptions,
        codec: Arc<dyn IcebergFileIOCodec>,
    );

    /// Registers the Iceberg integration with custom file IO serialization.
    fn with_iceberg_integration_with_file_io_codec(
        mut self,
        options: IcebergIntegrationOptions,
        codec: Arc<dyn IcebergFileIOCodec>,
    ) -> Self {
        self.set_iceberg_integration_with_file_io_codec(options, codec);
        self
    }
}

trait IcebergConfigExt {
    fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize);
    fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool);
    fn set_iceberg_row_selection_enabled(&mut self, enabled: bool);
    fn set_iceberg_column_stats_enabled(&mut self, enabled: bool);
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

    fn set_iceberg_column_stats_enabled(&mut self, enabled: bool) {
        iceberg_config_mut(self).column_stats_enabled = enabled;
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

fn set_iceberg_integration(
    state: &mut SessionState,
    options: IcebergIntegrationOptions,
    file_io_codec: Option<Arc<dyn IcebergFileIOCodec>>,
) {
    iceberg_config_mut(state.config_mut());
    let codec = LazyIcebergCodec::new(
        Arc::clone(&options.storage_factory),
        options.iceberg_runtime.clone(),
    );
    let assigned_codec = EagerIcebergCodec::new(
        Arc::clone(&options.storage_factory),
        options.iceberg_runtime.clone(),
    );
    let assigned_codec = match &file_io_codec {
        Some(file_io_codec) => assigned_codec.with_file_io_codec(Arc::clone(file_io_codec)),
        None => assigned_codec,
    };
    let codec = match file_io_codec {
        Some(file_io_codec) => codec.with_file_io_codec(file_io_codec),
        None => codec,
    };
    state.table_factories_mut().insert(
        TABLE_FACTORY_IDENTIFIER.to_string(),
        Arc::new(IcebergTableProviderFactory::new_with_runtime(
            options.storage_factory,
            options.iceberg_runtime,
        )),
    );
    state.set_distributed_desired_task_count_handler(iceberg_desired_task_count);
    // Assigned sources specialize their file groups; only lazy sources register feeds.
    state.set_distributed_scale_up_leaf_node_handler(iceberg_scale_up);
    state.set_distributed_work_unit_feed(|plan: &DataSourceExec| {
        plan.data_source()
            .downcast_ref::<LazyIcebergDataSource>()
            .map(LazyIcebergDataSource::feed)
    });
    state.set_distributed_user_codec(codec);
    state.set_distributed_user_codec(assigned_codec);
}

fn set_iceberg_integration_on_builder(
    builder: &mut SessionStateBuilder,
    options: IcebergIntegrationOptions,
    file_io_codec: Option<Arc<dyn IcebergFileIOCodec>>,
) {
    iceberg_config_mut(builder.config().get_or_insert_default());
    let codec = LazyIcebergCodec::new(
        Arc::clone(&options.storage_factory),
        options.iceberg_runtime.clone(),
    );
    let assigned_codec = EagerIcebergCodec::new(
        Arc::clone(&options.storage_factory),
        options.iceberg_runtime.clone(),
    );
    let assigned_codec = match &file_io_codec {
        Some(file_io_codec) => assigned_codec.with_file_io_codec(Arc::clone(file_io_codec)),
        None => assigned_codec,
    };
    let codec = match file_io_codec {
        Some(file_io_codec) => codec.with_file_io_codec(file_io_codec),
        None => codec,
    };
    builder.table_factories().get_or_insert_default().insert(
        TABLE_FACTORY_IDENTIFIER.to_string(),
        Arc::new(IcebergTableProviderFactory::new_with_runtime(
            options.storage_factory,
            options.iceberg_runtime,
        )),
    );
    builder.set_distributed_desired_task_count_handler(iceberg_desired_task_count);
    // Same hooks as SessionState, registered before the builder creates the state.
    builder.set_distributed_scale_up_leaf_node_handler(iceberg_scale_up);
    builder.set_distributed_work_unit_feed(|plan: &DataSourceExec| {
        plan.data_source()
            .downcast_ref::<LazyIcebergDataSource>()
            .map(LazyIcebergDataSource::feed)
    });
    builder.set_distributed_user_codec(codec);
    builder.set_distributed_user_codec(assigned_codec);
}

impl IcebergExt for SessionStateBuilder {
    fn set_iceberg_integration(&mut self, options: IcebergIntegrationOptions) {
        set_iceberg_integration_on_builder(self, options, None);
    }

    delegate! {
        to self.config().get_or_insert_default() {
            fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize);
            fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool);
            fn set_iceberg_row_selection_enabled(&mut self, enabled: bool);
            fn set_iceberg_column_stats_enabled(&mut self, enabled: bool);
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

            #[call(set_iceberg_column_stats_enabled)]
            #[expr($;self)]
            fn with_iceberg_column_stats_enabled(mut self, enabled: bool) -> Self;
        }
    }
}

impl IcebergExt for SessionState {
    fn set_iceberg_integration(&mut self, options: IcebergIntegrationOptions) {
        set_iceberg_integration(self, options, None);
    }

    delegate! {
        to self.config_mut() {
            fn set_iceberg_data_file_concurrency_limit(&mut self, limit: usize);
            fn set_iceberg_row_group_filtering_enabled(&mut self, enabled: bool);
            fn set_iceberg_row_selection_enabled(&mut self, enabled: bool);
            fn set_iceberg_column_stats_enabled(&mut self, enabled: bool);
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

            #[call(set_iceberg_column_stats_enabled)]
            #[expr($;self)]
            fn with_iceberg_column_stats_enabled(mut self, enabled: bool) -> Self;
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
            fn set_iceberg_column_stats_enabled(&mut self, enabled: bool);
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

            #[call(set_iceberg_column_stats_enabled)]
            #[expr($;self)]
            fn with_iceberg_column_stats_enabled(mut self, enabled: bool) -> Self;
        }
    }
}

impl IcebergFileIOExt for SessionStateBuilder {
    fn set_iceberg_integration_with_file_io_codec(
        &mut self,
        options: IcebergIntegrationOptions,
        codec: Arc<dyn IcebergFileIOCodec>,
    ) {
        set_iceberg_integration_on_builder(self, options, Some(codec));
    }
}

impl IcebergFileIOExt for SessionState {
    fn set_iceberg_integration_with_file_io_codec(
        &mut self,
        options: IcebergIntegrationOptions,
        codec: Arc<dyn IcebergFileIOCodec>,
    ) {
        set_iceberg_integration(self, options, Some(codec));
    }
}

impl IcebergFileIOExt for SessionContext {
    delegate! {
        to self.state_ref().write() {
            fn set_iceberg_integration_with_file_io_codec(
                &mut self,
                options: IcebergIntegrationOptions,
                codec: Arc<dyn IcebergFileIOCodec>,
            );
        }
    }
}
