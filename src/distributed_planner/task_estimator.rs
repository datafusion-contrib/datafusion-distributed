use crate::{ChannelResolver, DistributedConfig, PartitionIsolatorExec};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionConfig;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

/// Result of running a [TaskEstimator] on a leaf node. It tells the distributed planner hints
/// about how many tasks should be employed in [Stage]s that contain leaf nodes.
pub struct TaskEstimation {
    /// The amount of tasks that should be used in the [Stage] containing the leaf node.
    ///
    /// Even if implementations get to decide this number, there are situations where it can
    /// get overridden:
    /// - If a [Stage] contains multiple leaf nodes, the one that declares the biggest
    ///   task_count wins.
    /// - If there are less available workers than this number, the amount of available workers
    ///   is chosen.
    pub task_count: usize,
    /// If this is set to something, the leaf node will get replaced by this.
    ///
    /// This can be used by [TaskEstimator] implementations to perform transformations like:
    /// - repartitioning the leaf node.
    /// - wrapping it with a [PartitionIsolatorExec].
    /// - any other arbitrary modification.
    pub new_plan: Option<Arc<dyn ExecutionPlan>>,
}

/// Given a leaf node, provides an estimation about how many tasks should be used in the
/// stage containing it, and if the leaf node should be replaced by some other.
///
/// The distributed planner will try many [TaskEstimator]s in order until one provides an
/// estimation for a specific leaf node. Once that's done, upper stages will get its task
/// count calculated based on wether lower stages are reducing the cardinality of the data
/// or increasing it.
pub trait TaskEstimator {
    /// Function applied to leaf nodes that returns a [TaskEstimation] hinting how many
    /// tasks should be used in the [Stage] containing that leaf node, and if the leaf
    /// node itself should be modified somehow.
    fn estimate_tasks(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation>;
}

impl TaskEstimator for usize {
    fn estimate_tasks(
        &self,
        _: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        Some(TaskEstimation {
            task_count: *self,
            new_plan: None,
        })
    }
}

impl TaskEstimator for Arc<dyn TaskEstimator> {
    fn estimate_tasks(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        self.as_ref().estimate_tasks(plan, cfg)
    }
}

impl TaskEstimator for Arc<dyn TaskEstimator + Send + Sync> {
    fn estimate_tasks(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        self.as_ref().estimate_tasks(plan, cfg)
    }
}

impl<F: Fn(&Arc<dyn ExecutionPlan>, &ConfigOptions) -> Option<TaskEstimation>> TaskEstimator for F {
    fn estimate_tasks(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        self(plan, cfg)
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
        opts.extensions.insert(DistributedConfig {
            __private_task_estimator: estimators,
            ..Default::default()
        });
    }
}

#[derive(Debug)]
struct FileScanConfigTaskEstimator;

impl TaskEstimator for FileScanConfigTaskEstimator {
    fn estimate_tasks(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let d_cfg = cfg.extensions.get::<DistributedConfig>()?;
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let file_scan: &FileScanConfig = dse.data_source().as_any().downcast_ref()?;

        // Count how many distinct files we have in the FileScanConfig.
        let mut distinct_files = HashSet::new();
        for file_group in &file_scan.file_groups {
            for file in file_group.iter() {
                distinct_files.insert(file.object_meta.location.clone());
            }
        }
        let distinct_files = distinct_files.len();

        // Based on the user-provided files_per_task configuration, do the math to calculate
        // how many tasks should be used, without surpassing the number of available workers.
        let mut task_count = distinct_files.div_ceil(d_cfg.files_per_task);
        let workers = match d_cfg.__private_channel_resolver.0.get_urls() {
            Ok(urls) => urls.len(),
            Err(_) => 1,
        };
        task_count = task_count.min(workers);

        // Based on the task count, attempt to scale up the partitions in the DataSourceExec by
        // repartitioning it. This will result in a DataSourceExec with potentially a lot of
        // partitions, but as we are going wrap it with PartitionIsolatorExec that's fine.
        let scaled_partitions = task_count * plan.output_partitioning().partition_count();
        let mut plan = Arc::clone(plan);
        if let Ok(Some(repartitioned)) = plan.repartitioned(scaled_partitions, cfg) {
            plan = repartitioned;
        }
        plan = Arc::new(PartitionIsolatorExec::new(plan));

        Some(TaskEstimation {
            task_count,
            new_plan: Some(plan),
        })
    }
}

#[derive(Clone, Default)]
pub(crate) struct CombinedTaskEstimator {
    pub(crate) user_provided: Vec<Arc<dyn TaskEstimator + Send + Sync>>,
}

impl TaskEstimator for CombinedTaskEstimator {
    fn estimate_tasks(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        for estimator in &self.user_provided {
            if let Some(result) = estimator.estimate_tasks(plan, cfg) {
                return Some(result);
            }
        }
        // We want to execute the default estimators last so that the user-provided ones have
        // a chance of providing an estimation.
        // If none of the user-provided returned an estimation, the default ones are used.
        for default_estimator in [&FileScanConfigTaskEstimator as &dyn TaskEstimator] {
            if let Some(result) = default_estimator.estimate_tasks(plan, cfg) {
                return Some(result);
            }
        }
        None
    }
}
