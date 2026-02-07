use crate::config_extension_ext::set_distributed_option_extension;
use crate::{DistributedConfig, PartitionIsolatorExec};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use delegate::delegate;
use std::fmt::Debug;
use std::sync::Arc;

/// Annotation attached to a single [ExecutionPlan] that determines how many distributed tasks
/// it should run on.
#[derive(Debug, Clone)]
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
            Self::Desired(desired) => *desired,
            Self::Maximum(maximum) => *maximum,
        }
    }

    pub(crate) fn limit(self, limit: usize) -> Self {
        match self {
            Self::Desired(desired) => Self::Desired(desired.min(limit)),
            Self::Maximum(maximum) => Self::Maximum(maximum.min(limit)),
        }
    }
}

/// The keys (if any) which each stage is partitioned by, rather than within each task which
/// single-node DataFusion provides with a distribution requirement.
#[derive(Debug, Clone)]
pub enum StagePartitioning {
    /// No distribution specified.
    Unspecified,
    /// There is only a single task, thus it's trivially disjoint.
    Single,
    /// Tasks are partitioned by the given keys.
    Hash(Vec<Arc<dyn PhysicalExpr>>),
}

impl Default for StagePartitioning {
    fn default() -> Self {
        Self::Unspecified
    }
}

impl StagePartitioning {
    pub fn as_distribution(&self) -> datafusion::physical_expr::Distribution {
        match self {
            StagePartitioning::Unspecified => {
                datafusion::physical_expr::Distribution::UnspecifiedDistribution
            }
            StagePartitioning::Single => datafusion::physical_expr::Distribution::SinglePartition,
            StagePartitioning::Hash(keys) => {
                datafusion::physical_expr::Distribution::HashPartitioned(keys.clone())
            }
        }
    }

    pub fn as_partitioning(&self) -> Option<datafusion::physical_expr::Partitioning> {
        match self {
            StagePartitioning::Unspecified => None,
            StagePartitioning::Single => {
                Some(datafusion::physical_expr::Partitioning::UnknownPartitioning(1))
            }
            StagePartitioning::Hash(keys) => Some(datafusion::physical_expr::Partitioning::Hash(
                keys.clone(),
                1,
            )),
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
    /// The distribution of the data across tasks at the leaf nodes. This will be propagated,
    /// modified, and used throughout the rest of the stages as well.
    pub stage_partitioning: StagePartitioning,
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
            stage_partitioning: StagePartitioning::Unspecified,
        }
    }

    pub fn maximum_with_stage_partitioning(
        value: usize,
        stage_partitioning: StagePartitioning,
    ) -> Self {
        TaskEstimation {
            task_count: TaskCountAnnotation::Maximum(value),
            stage_partitioning,
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
            stage_partitioning: StagePartitioning::Unspecified,
        }
    }

    pub fn desired_with_stage_partitioning(
        value: usize,
        stage_partitioning: StagePartitioning,
    ) -> Self {
        TaskEstimation {
            task_count: TaskCountAnnotation::Desired(value),
            stage_partitioning,
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
    ) -> Option<Arc<dyn ExecutionPlan>>;
}

impl TaskEstimator for usize {
    fn task_estimation(
        &self,
        inputs: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        if inputs.children().is_empty() {
            Some(TaskEstimation::desired(*self))
        } else {
            None
        }
    }

    fn scale_up_leaf_node(
        &self,
        _: &Arc<dyn ExecutionPlan>,
        _: usize,
        _: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}

impl TaskEstimator for Arc<dyn TaskEstimator> {
    delegate! {
        to self.as_ref() {
            fn task_estimation(&self, plan: &Arc<dyn ExecutionPlan>, cfg: &ConfigOptions) -> Option<TaskEstimation>;
            fn scale_up_leaf_node(&self, plan: &Arc<dyn ExecutionPlan>, task_count: usize, cfg: &ConfigOptions) -> Option<Arc<dyn ExecutionPlan>>;
        }
    }
}

impl TaskEstimator for Arc<dyn TaskEstimator + Send + Sync> {
    delegate! {
        to self.as_ref() {
            fn task_estimation(&self, plan: &Arc<dyn ExecutionPlan>, cfg: &ConfigOptions) -> Option<TaskEstimation>;
            fn scale_up_leaf_node(&self, plan: &Arc<dyn ExecutionPlan>, task_count: usize, cfg: &ConfigOptions) -> Option<Arc<dyn ExecutionPlan>>;
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
/// [FileScanConfig]s data sources (e.g., Parquet or CSV files). it will read the
/// [DistributedConfig].`files_per_task` field and assigns as many tasks as needed so that
/// no task handles more than the configured files.
#[derive(Debug)]
struct FileScanConfigTaskEstimator;

impl TaskEstimator for FileScanConfigTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let file_scan: &FileScanConfig = dse.data_source().as_any().downcast_ref()?;

        let d_cfg = cfg.extensions.get::<DistributedConfig>()?;

        // Count how many partitioned files we have in the FileScanConfig.
        let mut partitioned_files = 0;
        for file_group in &file_scan.file_groups {
            partitioned_files += file_group.len();
        }

        // Based on the user-provided files_per_task configuration, do the math to calculate
        // how many tasks should be used, without surpassing the number of available workers.
        let task_count = partitioned_files.div_ceil(d_cfg.files_per_task);

        // If the scan already exposes hash partitioning, seed stage partitioning from it.
        // Skip this when a user-provided estimator exists so manual partitioning wins.
        let stage_partitioning = if d_cfg.__private_task_estimator.user_provided.is_empty() {
            match plan.output_partitioning() {
                datafusion::physical_expr::Partitioning::Hash(keys, _) if !keys.is_empty() => {
                    StagePartitioning::Hash(keys.clone())
                }
                _ => StagePartitioning::Unspecified,
            }
        } else {
            StagePartitioning::Unspecified
        };

        Some(TaskEstimation::desired_with_stage_partitioning(
            task_count,
            stage_partitioning,
        ))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        if task_count == 1 {
            return Some(Arc::clone(plan));
        }
        // Based on the task count, attempt to scale up the partitions in the DataSourceExec by
        // repartitioning it. This will result in a DataSourceExec with potentially a lot of
        // partitions, but as we are going to wrap it with PartitionIsolatorExec, that's fine.
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let file_scan: &FileScanConfig = dse.data_source().as_any().downcast_ref()?;

        let mut new_file_scan = file_scan.clone();
        new_file_scan.file_groups.clear();
        for file_group in file_scan.file_groups.clone() {
            new_file_scan
                .file_groups
                .extend(file_group.split_files(task_count));
        }
        let plan = DataSourceExec::from_data_source(new_file_scan);
        Some(Arc::new(PartitionIsolatorExec::new(plan, task_count)))
    }
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
    ) -> Option<Arc<dyn ExecutionPlan>> {
        for estimator in &self.user_provided {
            if let Some(result) = estimator.scale_up_leaf_node(plan, task_count, cfg) {
                return Some(result);
            }
        }
        // We want to execute the default estimators last so that the user-provided ones have
        // a chance of providing an estimation.
        // If none of the user-provided returned an estimation, the default ones are used.
        for default_estimator in [&FileScanConfigTaskEstimator as &dyn TaskEstimator] {
            if let Some(result) = default_estimator.scale_up_leaf_node(plan, task_count, cfg) {
                return Some(result);
            }
        }
        None
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

        let node = make_data_source_exec().await?;
        assert_eq!(combined.task_count(node, |cfg| cfg), 3);
        Ok(())
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
            let d_cfg = DistributedConfig {
                files_per_task: 1,
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
        ) -> Option<Arc<dyn ExecutionPlan>> {
            None
        }
    }
}
