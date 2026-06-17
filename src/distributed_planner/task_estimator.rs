use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use crate::execution_plans::DistributedLeafExec;
use TaskCountAnnotation::*;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileGroup, FileGroupPartitioner, FileScanConfig};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use delegate::delegate;
use std::fmt::Debug;
use std::sync::Arc;
use url::Url;

/// Annotation attached to a single [ExecutionPlan] that determines how many distributed tasks
/// it should run on.
#[derive(Debug, Clone, Copy)]
pub enum TaskCountAnnotation {
    /// The desired number of distributed tasks for this node. The final task count for the
    /// annotated node might not be exactly this number, it is more like a hint, so depending
    /// on the desired task count of adjacent nodes, the final task count might change.
    Desired(usize),
    /// Sets a maximum number of distributed tasks for this node. Typically used with the inner
    /// value of 1, stating that this node cannot be executed in a distributed fashion.
    Maximum(usize),
}

impl From<TaskCountAnnotation> for usize {
    fn from(annotation: TaskCountAnnotation) -> Self {
        annotation.as_usize()
    }
}

impl TaskCountAnnotation {
    pub fn as_usize(&self) -> usize {
        match self {
            Desired(desired) => *desired,
            Maximum(maximum) => *maximum,
        }
    }

    pub(crate) fn limit(self, limit: usize) -> Self {
        match self {
            Desired(desired) => Desired(desired.min(limit)),
            Maximum(maximum) => Maximum(maximum.min(limit)),
        }
    }

    pub(crate) fn merge(self, other: TaskCountAnnotation) -> Self {
        match (self, other) {
            (Desired(a), Desired(b)) => Desired(std::cmp::max(a, b)),
            (Desired(_), Maximum(b)) => Maximum(b),
            (Maximum(a), Desired(_)) => Maximum(a),
            (Maximum(a), Maximum(b)) => Maximum(std::cmp::min(a, b)),
        }
    }
}

/// Result of running a [TaskEstimator] on a leaf node. It tells the distributed planner hints
/// about how many tasks should be used in [Stage]s that contain leaf nodes.
pub struct TaskEstimation {
    /// The number of tasks that should be used in the [Stage] containing the leaf node.
    ///
    /// Even if implementations get to decide this number, there are situations where it can
    /// get overridden:
    /// - If a [Stage] contains multiple leaf nodes, the one that declares the biggest
    ///   task_count wins.
    /// - If there are less available workers than this number, the number of available workers
    ///   is chosen.
    pub task_count: TaskCountAnnotation,
}

impl TaskEstimation {
    /// Tells the distributed planner that the evaluated stage can have **at maximum** the provided
    /// number of tasks, setting a hard upper limit.
    ///
    /// Returning `TaskEstimation::maximum(1)` tells the distributed planner that the evaluated
    /// stage cannot be distributed.
    ///
    /// Even if a `TaskEstimation::maximum(N)` is provided, any other node in the same stage
    /// providing a value of `TaskEstimation::maximum(M)` where `M` < `N` will have preference.
    pub fn maximum(value: usize) -> Self {
        TaskEstimation {
            task_count: TaskCountAnnotation::Maximum(value),
        }
    }

    /// Tells the distributed planner that the evaluated can **optimally** have the provided
    /// number of tasks, setting a soft task count hint that can be overridden by others.
    ///
    /// The provided `TaskEstimation::desired(N)` can be overridden by:
    /// - Other nodes providing a `TaskEstimation::desired(M)` where `M` > `N`.
    /// - Any other node providing a `TaskEstimation::maximum(M)` where `M` can be anything.
    pub fn desired(value: usize) -> Self {
        TaskEstimation {
            task_count: TaskCountAnnotation::Desired(value),
        }
    }
}

/// Given a leaf node, provides an estimation about how many tasks should be used in the
/// stage containing it, and if the leaf node should be replaced by some other.
///
/// The distributed planner will try many [TaskEstimator]s in order until one provides an
/// estimation for a specific leaf node. Once that's done, upper stages will get their task
/// count calculated based on whether lower stages are reducing the cardinality of the data
/// or increasing it.
pub trait TaskEstimator {
    /// Function applied to each node that returns a [TaskEstimation] hinting how many
    /// tasks should be used in the [Stage] containing that node.
    ///
    /// All the [TaskEstimator] registered in the session will be applied to the node
    /// until one returns an estimation.
    ///
    ///
    /// If no estimation is returned from any of the registered [TaskEstimator]s, then:
    /// - If the node is a leaf node,`Maximum(1)` is assumed, hinting the distributed planner
    ///   that the leaf node cannot be distributed across tasks.
    /// - If the node is a normal node in the plan, then the maximum task count from its children
    ///   is inherited.
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation>;

    /// After a final task_count is decided, taking into account all the leaf nodes in the [Stage],
    /// this allows performing a transformation in the leaf nodes for accounting for the fact that
    /// they are going to run in multiple tasks.
    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        cfg: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;

    /// Optionally defines a custom protocol for routing tasks to specific worker URLs. Receives
    /// routing context including task count and a list of available URLs, and returns a vector
    /// of routed URLs, in order of task assignment.
    ///
    /// If Ok(Some(Vec<Url>)) is returned, tasks are sent in order to the URLs specified in the
    /// returned vector. If Ok(None) is returned, execution defaults to round-robin routing.
    fn route_tasks(&self, _routing_ctx: &TaskRoutingContext<'_>) -> Result<Option<Vec<Url>>> {
        Ok(None)
    }
}

/// Context usable for routing tasks to worker URLs.
pub struct TaskRoutingContext<'a> {
    /// The task context active at routing time.
    pub task_ctx: Arc<TaskContext>,
    /// The head execution plan of the stage being routed.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// The number of tasks to be assigned.
    pub task_count: usize,
}

impl TaskEstimator for usize {
    fn task_estimation(
        &self,
        inputs: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        if inputs.children().is_empty() {
            Some(TaskEstimation {
                task_count: TaskCountAnnotation::Desired(*self),
            })
        } else {
            None
        }
    }

    fn scale_up_leaf_node(
        &self,
        _: &Arc<dyn ExecutionPlan>,
        _: usize,
        _: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }
}

impl TaskEstimator for Arc<dyn TaskEstimator> {
    delegate! {
        to self.as_ref() {
            fn task_estimation(&self, plan: &Arc<dyn ExecutionPlan>, cfg: &ConfigOptions) -> Option<TaskEstimation>;
            fn scale_up_leaf_node(&self, plan: &Arc<dyn ExecutionPlan>, task_count: usize, cfg: &ConfigOptions) -> Result<Option<Arc<dyn ExecutionPlan>>>;
            fn route_tasks(&self, routing_ctx: &TaskRoutingContext<'_>) -> Result<Option<Vec<Url>>>;
        }
    }
}

impl TaskEstimator for Arc<dyn TaskEstimator + Send + Sync> {
    delegate! {
        to self.as_ref() {
            fn task_estimation(&self, plan: &Arc<dyn ExecutionPlan>, cfg: &ConfigOptions) -> Option<TaskEstimation>;
            fn scale_up_leaf_node(&self, plan: &Arc<dyn ExecutionPlan>, task_count: usize, cfg: &ConfigOptions) -> Result<Option<Arc<dyn ExecutionPlan>>>;
            fn route_tasks(&self, routing_ctx: &TaskRoutingContext<'_>) -> Result<Option<Vec<Url>>>;
        }
    }
}

pub(crate) fn set_distributed_task_estimator(
    cfg: &mut SessionConfig,
    estimator: impl TaskEstimator + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg
            .__private_task_estimator
            .user_provided
            .push(Arc::new(estimator));
    } else {
        let mut estimators = CombinedTaskEstimator::default();
        estimators.user_provided.push(Arc::new(estimator));
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_task_estimator: estimators,
                ..Default::default()
            },
        )
    }
}

/// [TaskEstimator] implementation that acts on [DataSourceExec] nodes that contain
/// [FileScanConfig]s data sources (e.g., Parquet or CSV files). It reads the
/// [DistributedConfig].`file_scan_config_bytes_per_partition` field and assigns as many tasks as
/// needed so that no partition scans more than the configured number of bytes.
#[derive(Debug)]
pub(crate) struct FileScanConfigTaskEstimator;

impl TaskEstimator for FileScanConfigTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let dse: &DataSourceExec = plan.downcast_ref()?;
        let file_scan: &FileScanConfig = dse.data_source().downcast_ref()?;

        let d_cfg = cfg.extensions.get::<DistributedConfig>()?;

        let mut total_bytes = 0;
        for file_group in &file_scan.file_groups {
            for file in file_group.files() {
                total_bytes += file.effective_size() as usize
            }
        }

        let task_count = total_bytes
            .div_ceil(d_cfg.file_scan_config_bytes_per_partition)
            .div_ceil(cfg.execution.target_partitions);

        Some(TaskEstimation::desired(task_count))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(dse) = plan.downcast_ref::<DataSourceExec>() else {
            return Ok(None);
        };
        let Some(file_scan) = dse.data_source().downcast_ref::<FileScanConfig>() else {
            return Ok(None);
        };
        let partition_count = plan.output_partitioning().partition_count();

        let rebalanced = if file_scan.partitioned_by_file_group {
            let all_partitioned_files = file_scan
                .file_groups
                .iter()
                .flat_map(|file_group| file_group.iter().cloned())
                .collect::<Vec<_>>();
            rebalance_round_robin(all_partitioned_files, partition_count * task_count)
                .into_iter()
                .map(FileGroup::new)
                .collect::<Vec<_>>()
        } else {
            FileGroupPartitioner::new()
                .with_target_partitions(partition_count * task_count)
                // Allow repartitioning beyond normal limits, putting the limit in
                // `partition_count * task_count` target partitions, and not in the
                // resulting size.
                .with_repartition_file_min_size(0)
                .with_preserve_order_within_groups(!file_scan.output_ordering.is_empty())
                .repartition_file_groups(&file_scan.file_groups)
                .unwrap_or_else(|| file_scan.file_groups.clone())
                .into_iter()
                .collect()
        };

        let mut file_scan_template = file_scan.clone();
        file_scan_template.file_groups.clear();
        let mut file_scans = vec![file_scan_template; task_count];
        for (i, file_group) in rebalanced.into_iter().enumerate() {
            file_scans[i % task_count].file_groups.push(file_group);
        }

        let dle = DistributedLeafExec::try_new(
            Arc::clone(plan),
            file_scans
                .into_iter()
                .map(|file_scan| DataSourceExec::from_data_source(file_scan) as _),
        )?;

        Ok(Some(Arc::new(dle)))
    }
}

fn rebalance_round_robin<T>(items: Vec<T>, target_groups: usize) -> Vec<Vec<T>> {
    let mut groups = (0..target_groups)
        .map(|_| Vec::new())
        .collect::<Vec<Vec<T>>>();
    for (idx, item) in items.into_iter().enumerate() {
        groups[idx % target_groups].push(item);
    }
    groups
}

/// Tries multiple user-provided [TaskEstimator]s until one returns an estimation. If none
/// returns an estimation, a set of default [TaskEstimation] implementations is tried. Right
/// now the only default [TaskEstimation] is [FileScanConfigTaskEstimator].
#[derive(Clone, Default)]
pub(crate) struct CombinedTaskEstimator {
    pub(crate) user_provided: Vec<Arc<dyn TaskEstimator + Send + Sync>>,
}

impl TaskEstimator for CombinedTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        for estimator in &self.user_provided {
            if let Some(result) = estimator.task_estimation(plan, cfg) {
                return Some(result);
            }
        }
        // We want to execute the default estimators last so that the user-provided ones have
        // a chance of providing an estimation.
        // If none of the user-provided returned an estimation, the default ones are used.
        for default_estimator in [&FileScanConfigTaskEstimator as &dyn TaskEstimator] {
            if let Some(result) = default_estimator.task_estimation(plan, cfg) {
                return Some(result);
            }
        }
        None
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        cfg: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        for estimator in &self.user_provided {
            if let Some(result) = estimator.scale_up_leaf_node(plan, task_count, cfg)? {
                return Ok(Some(result));
            }
        }
        // We want to execute the default estimators last so that the user-provided ones have
        // a chance of providing an estimation.
        // If none of the user-provided returned an estimation, the default ones are used.
        for default_estimator in [&FileScanConfigTaskEstimator as &dyn TaskEstimator] {
            if let Some(result) = default_estimator.scale_up_leaf_node(plan, task_count, cfg)? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }

    fn route_tasks(&self, routing_ctx: &TaskRoutingContext<'_>) -> Result<Option<Vec<Url>>> {
        for estimator in &self.user_provided {
            if let Some(result) = estimator.route_tasks(routing_ctx)? {
                return Ok(Some(result));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::networking::WorkerResolverExtension;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::test_utils::parquet::register_parquet_tables;
    use datafusion::error::DataFusionError;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_first_user_estimator_wins() -> Result<(), DataFusionError> {
        let mut combined = CombinedTaskEstimator::default();
        combined.push(10);
        combined.push(20);

        let node = make_data_source_exec().await?;
        assert_eq!(combined.task_count(node, |cfg| cfg), 10);
        Ok(())
    }

    #[tokio::test]
    async fn test_continues_until_some() -> Result<(), DataFusionError> {
        let mut combined = CombinedTaskEstimator::default();
        combined.push(|_: &Arc<dyn ExecutionPlan>, _: &ConfigOptions| None);
        combined.push(30);

        let node = make_data_source_exec().await?;
        assert_eq!(combined.task_count(node, |cfg| cfg), 30);
        Ok(())
    }

    #[tokio::test]
    async fn test_defaults_to_file_scan_config_task_estimator() -> Result<(), DataFusionError> {
        let mut combined = CombinedTaskEstimator::default();
        combined.push(|_: &Arc<dyn ExecutionPlan>, _: &ConfigOptions| None);

        // No user estimator returns a value, so the default FileScanConfigTaskEstimator kicks in.
        // Size the per-partition budget (with target_partitions pinned to 1) so the scan splits
        // into exactly 3 partitions.
        let node = make_data_source_exec().await?;
        let bytes_per_partition = total_scan_bytes(&node).div_ceil(3);
        let task_count = combined.task_count(node, |mut cfg| {
            cfg.file_scan_config_bytes_per_partition = bytes_per_partition;
            cfg
        });
        assert_eq!(task_count, 3);
        Ok(())
    }

    fn total_scan_bytes(node: &Arc<dyn ExecutionPlan>) -> usize {
        let dse = node.downcast_ref::<DataSourceExec>().unwrap();
        let file_scan = dse.data_source().downcast_ref::<FileScanConfig>().unwrap();
        file_scan
            .file_groups
            .iter()
            .flat_map(|file_group| file_group.files())
            .map(|file| file.effective_size() as usize)
            .sum()
    }

    #[test]
    fn test_rebalance_round_robin_fixes_group_boundary_skew() {
        let items = (0..8).collect::<Vec<_>>();
        let groups = rebalance_round_robin(items, 5);
        let sizes = groups.iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(sizes, vec![2, 2, 2, 1, 1]);
    }

    #[test]
    fn test_rebalance_round_robin_pads_with_empty_groups() {
        // With fewer items than target groups, the extra groups are kept empty rather than
        // dropped. This guarantees every task ends up with the same number of partitions.
        let items = vec![10, 20, 30];
        let groups = rebalance_round_robin(items, 5);
        let sizes = groups.iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(sizes, vec![1, 1, 1, 0, 0]);
    }

    impl CombinedTaskEstimator {
        fn push(&mut self, value: impl TaskEstimator + Send + Sync + 'static) {
            self.user_provided.push(Arc::new(value));
        }

        fn task_count(
            &self,
            node: Arc<dyn ExecutionPlan>,
            f: impl FnOnce(DistributedConfig) -> DistributedConfig,
        ) -> usize {
            let mut cfg = ConfigOptions::default();
            // Pin target_partitions so the byte-based estimation is deterministic regardless of
            // the host's core count.
            cfg.execution.target_partitions = 1;
            let d_cfg = DistributedConfig {
                file_scan_config_bytes_per_partition: 1,
                __private_worker_resolver: WorkerResolverExtension(Arc::new(
                    InMemoryWorkerResolver::new(3),
                )),
                ..Default::default()
            };
            cfg.extensions.insert(f(d_cfg));
            self.task_estimation(&node, &cfg)
                .unwrap()
                .task_count
                .as_usize()
        }
    }

    async fn make_data_source_exec() -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let ctx = SessionContext::new();
        register_parquet_tables(&ctx).await?;
        let mut plan = ctx
            .sql("SELECT * FROM weather")
            .await?
            .create_physical_plan()
            .await?;
        while !plan.children().is_empty() {
            plan = Arc::clone(plan.children()[0])
        }
        Ok(plan)
    }

    impl<F: Fn(&Arc<dyn ExecutionPlan>, &ConfigOptions) -> Option<TaskEstimation>> TaskEstimator for F {
        fn task_estimation(
            &self,
            plan: &Arc<dyn ExecutionPlan>,
            cfg: &ConfigOptions,
        ) -> Option<TaskEstimation> {
            self(plan, cfg)
        }

        fn scale_up_leaf_node(
            &self,
            _plan: &Arc<dyn ExecutionPlan>,
            _task_count: usize,
            _cfg: &ConfigOptions,
        ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
            Ok(None)
        }
    }
}
