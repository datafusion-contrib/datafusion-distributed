use crate::distributed_planner::distributed_planner_extension::CombinedDistributedPlannerExtension;
use crate::networking::{ChannelResolverExtension, WorkerResolverExtension};
use datafusion::common::{DataFusionError, extensions_options, not_impl_err, plan_err};
use datafusion::config::{ConfigExtension, ConfigField, ConfigOptions, Visit};
use std::fmt::{Debug, Formatter};

extensions_options! {
    /// Configuration for the distributed planner.
    pub struct DistributedConfig {
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
        /// Enable broadcast joins for CollectLeft hash joins. When enabled, the build side of
        /// a CollectLeft join is broadcast to all consumer tasks.
        /// TODO: This option exists temporarily until we become smarter about when to actually
        /// use broadcasting like checking build side size.
        /// For now, broadcasting all CollectLeft joins is not always beneficial.
        pub broadcast_joins: bool, default = false
        /// The compression used for sending data over the network between workers.
        /// It can be set to either `zstd`, `lz4` or `none`.
        pub compression: String, default = "lz4".to_string()
        /// How many bytes is each partition expected to process. This is used to calculate how many
        /// partitions are suitable to be used for driving the query forward. If the result of the
        /// calculation is that more partitions than `datafusion.execution.target_partitions` are
        /// needed, the query will use more workers.
        pub bytes_processed_per_partition: usize, default = 8 * 1024 * 1024
        /// Distributed DataFusion relies on row count estimation in order to infer how many workers
        /// should be used in serving the query. Some plans might not implement any kind of row count
        /// estimation, and this parameter sets the default estimated row count for those plans.
        pub default_estimated_row_count: Option<usize>, default = Some(0)
        /// Collection of [DistributedPlannerExtension]s that will be applied to leaf nodes in order to
        /// estimate how many tasks should be spawned for the [Stage] containing the leaf node.
        pub(crate) __private_distributed_planner_extension: CombinedDistributedPlannerExtension, default = CombinedDistributedPlannerExtension::default()
        /// [ChannelResolver] implementation that tells the distributed planner information about
        /// the available workers ready to execute distributed tasks.
        pub(crate) __private_channel_resolver: ChannelResolverExtension, default = ChannelResolverExtension::default()
        /// [WorkerResolver] implementation that tells the distributed planner information about
        /// the available workers ready to execute distributed tasks.
        pub(crate) __private_worker_resolver: WorkerResolverExtension, default = WorkerResolverExtension::not_implemented()
    }
}

impl DistributedConfig {
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

// FIXME: Ideally, all this structs would be passed as
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

impl ConfigField for CombinedDistributedPlannerExtension {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {
        //nothing to do.
    }

    fn set(&mut self, _: &str, _: &str) -> Result<(), DataFusionError> {
        not_impl_err!("not implemented")
    }
}

impl Debug for CombinedDistributedPlannerExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CombinedDistributedPlannerExtension")
    }
}
