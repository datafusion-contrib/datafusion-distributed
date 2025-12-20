use crate::config_extension_ext::set_distributed_option_extension;
use crate::{DistributedConfig, PartitionIsolatorExec};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::collections::HashSet;
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

/// Given a leaf node, provides an estimation about how many tasks should be used in the
/// stage containing it, and if the leaf node should be replaced by some other.
///
/// The distributed planner will try many [TaskEstimator]s in order until one provides an
/// estimation for a specific leaf node. Once that's done, upper stages will get their task
/// count calculated based on whether lower stages are reducing the cardinality of the data
/// or increasing it.
pub trait TaskEstimator {
    /// Function applied to leaf nodes that returns a [TaskEstimation] hinting how many
    /// tasks should be used in the [Stage] containing that leaf node.
    fn tasks_for_leaf_node(
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
    fn tasks_for_leaf_node(
        &self,
        _: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        Some(TaskEstimation {
            task_count: TaskCountAnnotation::Desired(*self),
        })
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
    fn tasks_for_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        self.as_ref().tasks_for_leaf_node(plan, cfg)
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        self.as_ref().scale_up_leaf_node(plan, task_count, cfg)
    }
}

impl TaskEstimator for Arc<dyn TaskEstimator + Send + Sync> {
    fn tasks_for_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        self.as_ref().tasks_for_leaf_node(plan, cfg)
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        self.as_ref().scale_up_leaf_node(plan, task_count, cfg)
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
        set_distributed_option_extension(cfg, DistributedConfig {
            __private_task_estimator: estimators,
            ..Default::default()
        }).expect("Calling set_distributed_option_extension with a default DistributedConfig should never fail");
    }
}

/// [TaskEstimator] implementation that acts on [DataSourceExec] nodes that contain
/// [FileScanConfig]s data sources (e.g., Parquet or CSV files). it will read the
/// [DistributedConfig].`files_per_task` field and assigns as many tasks as needed so that
/// no task handles more than the configured files.
#[derive(Debug)]
struct FileScanConfigTaskEstimator;

impl TaskEstimator for FileScanConfigTaskEstimator {
    fn tasks_for_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let d_cfg = cfg.extensions.get::<DistributedConfig>()?;
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let file_scan: &FileScanConfig = dse.data_source().as_any().downcast_ref()?;

        // Count how many distinct files we have in the FileScanConfig. Each file in each
        // file group is a PartitionedFile rather than a full file, so it's possible that
        // many entries refer to different chunks of the same physical file. By keeping a
        // HashSet of the different locations of the PartitionedFiles we count how many actual
        // different files we have.
        let mut distinct_files = HashSet::new();
        for file_group in &file_scan.file_groups {
            for file in file_group.iter() {
                distinct_files.insert(file.object_meta.location.clone());
            }
        }
        let distinct_files = distinct_files.len();

        // Based on the user-provided files_per_task configuration, do the math to calculate
        // how many tasks should be used, without surpassing the number of available workers.
        let task_count = distinct_files.div_ceil(d_cfg.files_per_task);

        Some(TaskEstimation {
            task_count: TaskCountAnnotation::Desired(task_count),
        })
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
        // partitions, but as we are going wrap it with PartitionIsolatorExec that's fine.
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
    fn tasks_for_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        for estimator in &self.user_provided {
            if let Some(result) = estimator.tasks_for_leaf_node(plan, cfg) {
                return Some(result);
            }
        }
        // We want to execute the default estimators last so that the user-provided ones have
        // a chance of providing an estimation.
        // If none of the user-provided returned an estimation, the default ones are used.
        for default_estimator in [&FileScanConfigTaskEstimator as &dyn TaskEstimator] {
            if let Some(result) = default_estimator.tasks_for_leaf_node(plan, cfg) {
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
    use crate::channel_resolver_ext::ChannelResolverExtension;
    use crate::test_utils::in_memory_channel_resolver::InMemoryChannelResolver;
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
                __private_channel_resolver: ChannelResolverExtension(Arc::new(
                    InMemoryChannelResolver::new(3),
                )),
                ..Default::default()
            };
            cfg.extensions.insert(f(d_cfg));
            self.tasks_for_leaf_node(&node, &cfg)
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
        fn tasks_for_leaf_node(
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
