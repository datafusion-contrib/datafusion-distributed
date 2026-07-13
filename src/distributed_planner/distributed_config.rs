use crate::config_extension_ext::set_distributed_option_extension;
use datafusion::common::{DataFusionError, extensions_options, plan_err};
use datafusion::config::{ConfigExtension, ConfigOptions};
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

extensions_options! {
    /// Configuration for the distributed planner.
    pub struct DistributedConfig {
        /// Sets the number of bytes each partitions is expected to scan from parquet files. If
        /// more partitions than the ones available in one machine would be needed, several machines
        /// are used, and the scan is distributed.
        /// Lowering this number will increase parallelism.
        pub file_scan_config_bytes_per_partition: usize, default = 16 * 1024 * 1024
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
        /// Calculates the task count of the different stages at execution time, based on runtime
        /// information collected by sampling at the head of the stages.
        ///
        /// With this option enabled, the shape of the distributed plan is only known after fully
        /// executing it, as it's dynamically created on the fly during execution.
        pub dynamic_task_count: bool, default = false
        /// If `dynamic_task_count` is enabled, this value is the amount of bytes each
        /// partition is expected to handle. Lower values will result in greater parallelism.
        pub dynamic_bytes_per_partition: usize, default = 16 * 1024 * 1024
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

    /// Gets the [DistributedConfig] from the [ConfigOptions]'s in the provided [SessionConfig].
    pub fn from_session_config(session_cfg: &SessionConfig) -> Result<&Self, DataFusionError> {
        Self::from_config_options(session_cfg.options())
    }

    /// Gets the [DistributedConfig] from the [ConfigOptions]'s in the provided [TaskContext].
    pub fn from_task_context(ctx: &Arc<TaskContext>) -> Result<&Self, DataFusionError> {
        Self::from_session_config(ctx.session_config())
    }

    /// Ensures that the [DistributedConfig] is present in the [SessionConfig]'s [ConfigOptions].
    /// If not, it will insert a default [DistributedConfig] into the [SessionConfig]'s [ConfigOptions].
    pub(crate) fn ensure_in_config(cfg: &mut SessionConfig) {
        if cfg
            .options()
            .extensions
            .get::<DistributedConfig>()
            .is_none()
        {
            set_distributed_option_extension(cfg, DistributedConfig::default())
        }
    }
}

impl ConfigExtension for DistributedConfig {
    const PREFIX: &'static str = "distributed";
}
