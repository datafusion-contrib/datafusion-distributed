use crate::channel_resolver_ext::ChannelResolverExtension;
use crate::distributed_planner::task_estimator::CombinedTaskEstimator;
use crate::{BoxCloneSyncChannel, ChannelResolver, TaskEstimator};
use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{DataFusionError, extensions_options, not_impl_err, plan_err};
use datafusion::config::{ConfigExtension, ConfigField, ConfigOptions, Visit};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use url::Url;

extensions_options! {
    /// Configuration for the distributed planner.
    pub struct DistributedConfig {
        /// Sets the maximum amount of files that will be assigned to each task. Reducing this
        /// number will spawn more tasks for the same number of files. This only applies when
        /// estimating tasks for stages containing `DataSourceExec` nodes with `FileScanConfig`
        /// implementations.
        pub files_per_task: usize, default = files_per_task_default()
        /// Task multiplying factor for when a node declares that it changes the cardinality
        /// of the data:
        /// - If a node is increasing the cardinality of the data, this factor will increase.
        /// - If a node reduces the cardinality of the data, this factor will decrease.
        /// - In any other situation, this factor is left intact.
        pub cardinality_task_count_factor: f64, default = cardinality_task_count_factor_default()
        /// Collection of [TaskEstimator]s that will be applied to leaf nodes in order to
        /// estimate how many tasks should be spawned for the [Stage] containing the leaf node.
        pub(crate) __private_task_estimator: CombinedTaskEstimator, default = CombinedTaskEstimator::default()
        /// [ChannelResolver] implementation that tells the distributed planner information about
        /// the available workers ready to execute distributed tasks.
        pub(crate) __private_channel_resolver: ChannelResolverExtension, default = ChannelResolverExtension::default()
    }
}

fn files_per_task_default() -> usize {
    if cfg!(test) || cfg!(feature = "integration") {
        1
    } else {
        get_available_parallelism()
    }
}

fn cardinality_task_count_factor_default() -> f64 {
    if cfg!(test) || cfg!(feature = "integration") {
        1.5
    } else {
        1.0
    }
}

impl DistributedConfig {
    /// Appends a [TaskEstimator] to the list. [TaskEstimator] will be executed sequentially in
    /// order on leaf nodes, and the first one to provide a value is the one that gets to decide
    /// how many tasks are used for that [Stage].
    pub fn with_task_estimator(
        mut self,
        task_estimator: impl TaskEstimator + Send + Sync + 'static,
    ) -> Self {
        self.__private_task_estimator
            .user_provided
            .push(Arc::new(task_estimator));
        self
    }

    /// Gets the [DistributedConfig] from the [ConfigOptions]'s extensions.
    pub fn from_config_options(cfg: &ConfigOptions) -> Result<&Self, DataFusionError> {
        let Some(distributed_cfg) = cfg.extensions.get::<DistributedConfig>() else {
            return plan_err!("DistributedConfig is not in ConfigOptions.extensions");
        };
        Ok(distributed_cfg)
    }

    /// Gets the [DistributedConfig] from the [ConfigOptions]'s extensions.
    pub fn from_config_options_mut(cfg: &mut ConfigOptions) -> Result<&mut Self, DataFusionError> {
        let Some(distributed_cfg) = cfg.extensions.get_mut::<DistributedConfig>() else {
            return plan_err!("DistributedConfig is not in ConfigOptions.extensions");
        };
        Ok(distributed_cfg)
    }
}

impl ConfigExtension for DistributedConfig {
    const PREFIX: &'static str = "distributed";
}

// FIXME: Ideally, both ChannelResolverExtension and TaskEstimators would be passed as
//  extensions in SessionConfig's AnyMap instead of the ConfigOptions. However, we need
//  to pass this as ConfigOptions as we need these two fields to be present during
//  planning in the DistributedPhysicalOptimizerRule, and the signature of the optimize()
//  method there accepts a ConfigOptions instead of a SessionConfig.
//  The following PR addresses this: https://github.com/apache/datafusion/pull/18168
//  but it still has not been accepted or merged.
//  Because of this, all the boilerplate trait implementations below are needed.
impl ConfigField for ChannelResolverExtension {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {
        // nothing to do.
    }

    fn set(&mut self, _: &str, _: &str) -> datafusion::common::Result<()> {
        not_impl_err!("Not implemented")
    }
}

impl Default for ChannelResolverExtension {
    fn default() -> Self {
        Self(Arc::new(NotImplementedChannelResolver))
    }
}

impl Debug for ChannelResolverExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChannelResolverExtension")
    }
}

struct NotImplementedChannelResolver;

#[async_trait]
impl ChannelResolver for NotImplementedChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        not_impl_err!("Not implemented")
    }

    async fn get_flight_client_for_url(
        &self,
        _: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        not_impl_err!("Not implemented")
    }
}

impl ConfigField for CombinedTaskEstimator {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {
        //nothing to do.
    }

    fn set(&mut self, _: &str, _: &str) -> Result<(), DataFusionError> {
        not_impl_err!("not implemented")
    }
}

impl Debug for CombinedTaskEstimator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskEstimators")
    }
}
