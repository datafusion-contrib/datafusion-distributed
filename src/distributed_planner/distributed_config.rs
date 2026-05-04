use crate::TaskEstimator;
use crate::distributed_planner::task_estimator::CombinedTaskEstimator;
use crate::networking::{ChannelResolverExtension, WorkerResolverExtension};
use crate::work_unit_feed::WorkUnitFeedRegistry;
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
        /// Enable broadcast joins for CollectLeft hash joins. When enabled, the build side of
        /// a CollectLeft join is broadcast to all consumer tasks.
        /// TODO: This option exists temporarily until we become smarter about when to actually
        /// use broadcasting like checking build side size.
        /// For now, broadcasting all CollectLeft joins is not always beneficial.
        pub broadcast_joins: bool, default = false
        /// The compression used for sending data over the network between workers.
        /// It can be set to either `zstd`, `lz4` or `none`.
        pub compression: String, default = "lz4".to_string()
        /// Overrides `datafusion.execution.batch_size` for worker-executed stages. Because
        /// `RepartitionExec` reads `session_config().batch_size()` at execute time to size its
        /// output batches (via its internal `LimitedBatchCoalescer`), this knob lets users tune
        /// shuffle batch sizes independently of the global `datafusion.execution.batch_size`.
        ///
        /// Set to 0 (the default) to apply no override and inherit `datafusion.execution.batch_size`.
        pub shuffle_batch_size: usize, default = 0
        /// Maximum tasks that will be assigned per stage during distributed planning.
        /// If set to 0, this value is the number of workers returned by the provided `WorkerResolver`.
        /// It defaults to 0.
        pub max_tasks_per_stage: usize, default = 0
        /// Enable the PartialReduce optimization, which inserts an extra aggregation pass
        /// above hash RepartitionExec before network shuffles to reduce shuffle data size.
        /// Disabled by default because its effectiveness is workload-dependent: it helps when
        /// aggregation significantly reduces cardinality, but adds overhead when it does not.
        pub partial_reduce: bool, default = false
        /// Soft byte budget that each per-worker connection will buffer in memory before pausing
        /// the gRPC pull from that worker. Per-partition channels are unbounded (to avoid
        /// head-of-line blocking between sibling partitions), so backpressure is enforced
        /// globally per [WorkerConnection] using this budget. A single message larger than this
        /// budget will still be admitted (otherwise we would livelock), so the actual peak per
        /// connection is `worker_connection_buffer_budget_bytes + max_message_size`.
        pub worker_connection_buffer_budget_bytes: usize, default = 64 * 1024 * 1024
        /// Collection of [TaskEstimator]s that will be applied to leaf nodes in order to
        /// estimate how many tasks should be spawned for the [Stage] containing the leaf node.
        pub(crate) __private_task_estimator: CombinedTaskEstimator, default = CombinedTaskEstimator::default()
        /// [ChannelResolver] implementation that tells the distributed planner information about
        /// the available workers ready to execute distributed tasks.
        pub(crate) __private_channel_resolver: ChannelResolverExtension, default = ChannelResolverExtension::default()
        /// [WorkerResolver] implementation that tells the distributed planner information about
        /// the available workers ready to execute distributed tasks.
        pub(crate) __private_worker_resolver: WorkerResolverExtension, default = WorkerResolverExtension::not_implemented()
        /// [WorkUnitFeedRegistry] that contains a set of getters that, applied to each node in a
        /// plan, will return the [crate::WorkUnitFeed]s present in all nodes.
        pub(crate) __private_work_unit_feed_registry: WorkUnitFeedRegistry, default = WorkUnitFeedRegistry::default()
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
//  planning in the DistributedQueryPlanner, and the signature of the create_physical_plan()
//  method there accepts a SessionState which only provides ConfigOptions.
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

impl ConfigField for WorkUnitFeedRegistry {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {
        //nothing to do.
    }

    fn set(&mut self, _: &str, _: &str) -> Result<(), DataFusionError> {
        not_impl_err!("not implemented")
    }
}

impl Debug for WorkUnitFeedRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkUnitFeedRegistry")
    }
}
