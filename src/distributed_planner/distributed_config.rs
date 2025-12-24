use crate::TaskEstimator;
use crate::distributed_planner::task_estimator::CombinedTaskEstimator;
use crate::networking::{ChannelResolverExtension, WorkerResolverExtension};
use datafusion::common::utils::get_available_parallelism;
use datafusion::common::{DataFusionError, extensions_options, not_impl_err, plan_err};
use datafusion::config::{ConfigExtension, ConfigField, ConfigOptions, Visit};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

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
        /// Upon shuffling over the network, data streams need to be disassembled in a lot of output
        /// partitions, which means the resulting streams might contain a lot of tiny record batches
        /// to be sent over the wire. This parameter controls the batch size in number of rows for
        /// the CoalesceBatchExec operator that is placed at the top of the stage for sending bigger
        /// batches over the wire.
        /// If set to 0, batch coalescing is disabled on network shuffle operations.
        pub shuffle_batch_size: usize, default = 8192
        /// When encountering a UNION operation, isolate its children depending on the task context.
        /// For example, on a UNION operation with 3 children running in 3 distributed tasks,
        /// instead of executing the 3 children in each 3 tasks with a DistributedTaskContext of
        /// 1/3, 2/3, and 3/3 respectively, Execute:
        /// - The first child in the first task with a DistributedTaskContext of 1/1
        /// - The second child in the second task with a DistributedTaskContext of 1/1
        /// - The third child in the third task with a DistributedTaskContext of 1/1
        pub children_isolator_unions: bool, default = true
        /// Propagate collected metrics from all nodes in the plan across network boundaries
        /// so that they can be reconstructed on the head node of the plan.
        pub collect_metrics: bool, default = true
        /// Collection of [TaskEstimator]s that will be applied to leaf nodes in order to
        /// estimate how many tasks should be spawned for the [Stage] containing the leaf node.
        pub(crate) __private_task_estimator: CombinedTaskEstimator, default = CombinedTaskEstimator::default()
        /// [ChannelResolver] implementation that tells the distributed planner information about
        /// the available workers ready to execute distributed tasks.
        pub(crate) __private_channel_resolver: ChannelResolverExtension, default = ChannelResolverExtension::default()
        /// [WorkerResolver] implementation that tells the distributed planner information about
        /// the available workers ready to execute distributed tasks.
        pub(crate) __private_worker_resolver: WorkerResolverExtension, default = WorkerResolverExtension::not_implemented()
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

impl Debug for ChannelResolverExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ChannelResolverExtension")
    }
}

impl ConfigField for WorkerResolverExtension {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {
        // nothing to do.
    }

    fn set(&mut self, _: &str, _: &str) -> datafusion::common::Result<()> {
        not_impl_err!("Not implemented")
    }
}

impl Debug for WorkerResolverExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkerResolverExtension")
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
