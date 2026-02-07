use crate::config_extension_ext::set_distributed_option_extension;
use crate::distributed_planner::statistics::ComputeCostClass;
use crate::{DistributedConfig, PartitionIsolatorExec};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use delegate::delegate;
use std::fmt::Debug;
use std::sync::Arc;

/// Extends the behavior of the distributed planner while interacting with one or more specific
/// [ExecutionPlan]s. Typically used for handling custom user defined [ExecutionPlan]s in a
/// distributed context.
pub trait DistributedPlannerExtension {
    /// Sets the compute cost of the provided [ExecutionPlan].
    ///
    /// This is useful when users have custom nodes, and they want to attribute a compute cost other
    /// than the default.
    fn compute_cost(
        &self,
        _plan: &Arc<dyn ExecutionPlan>,
        _cfg: &ConfigOptions,
    ) -> Option<ComputeCostClass> {
        None
    }

    /// Signals the distributed planner that the provided node must run in at most N tasks.
    /// Returning Some(1) effectively tells the planner that the node cannot be distributed.
    ///
    /// This is useful when users have custom nodes that cannot be executed in more than one task
    /// because of technical limitations. These nodes might still have the chance to be executed
    /// in a distributed query, but the stage that handles them will be force-limited to one task.
    fn max_tasks(&self, _plan: &Arc<dyn ExecutionPlan>, _cfg: &ConfigOptions) -> Option<usize> {
        None
    }

    /// Once the distributed planner has decided how many tasks should be used in each stage, it
    /// will call this method so that [ExecutionPlan]s have the chance to get "scaled up" to account
    /// for the provided `task_count`. For example, when reading from multiple partitioned files,
    /// these might get repartitioned further in order to account for it being run in multiple
    /// machines.
    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>>;
}

impl DistributedPlannerExtension for Arc<dyn DistributedPlannerExtension> {
    delegate! {
        to self.as_ref() {
            fn compute_cost(&self, _plan: &Arc<dyn ExecutionPlan>, _cfg: &ConfigOptions) -> Option<ComputeCostClass>;
            fn max_tasks(&self, plan: &Arc<dyn ExecutionPlan>, cfg: &ConfigOptions) -> Option<usize>;
            fn scale_up_leaf_node(&self, plan: &Arc<dyn ExecutionPlan>, task_count: usize, cfg: &ConfigOptions) -> Option<Arc<dyn ExecutionPlan>>;
        }
    }
}

impl DistributedPlannerExtension for Arc<dyn DistributedPlannerExtension + Send + Sync> {
    delegate! {
        to self.as_ref() {
            fn compute_cost(&self, _plan: &Arc<dyn ExecutionPlan>, _cfg: &ConfigOptions) -> Option<ComputeCostClass>;
            fn max_tasks(&self, plan: &Arc<dyn ExecutionPlan>, cfg: &ConfigOptions) -> Option<usize>;
            fn scale_up_leaf_node(&self, plan: &Arc<dyn ExecutionPlan>, task_count: usize, cfg: &ConfigOptions) -> Option<Arc<dyn ExecutionPlan>>;
        }
    }
}

pub(crate) fn set_distributed_planner_extension(
    cfg: &mut SessionConfig,
    estimator: impl DistributedPlannerExtension + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg
            .__private_distributed_planner_extension
            .user_provided
            .push(Arc::new(estimator));
    } else {
        let mut estimators = CombinedDistributedPlannerExtension::default();
        estimators.user_provided.push(Arc::new(estimator));
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_distributed_planner_extension: estimators,
                ..Default::default()
            },
        )
    }
}

/// [DistributedPlannerExtension] implementation that acts on [DataSourceExec] nodes that contain
/// [FileScanConfig]s data sources (e.g., Parquet or CSV files). it will read the
/// [DistributedConfig].`files_per_task` field and assigns as many tasks as needed so that
/// no task handles more than the configured files.
#[derive(Debug)]
struct FileScanConfigDistributedPlannerExtension;

impl DistributedPlannerExtension for FileScanConfigDistributedPlannerExtension {
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

/// Tries multiple user-provided [DistributedPlannerExtension]s until one returns an estimation. If none
/// returns an estimation, a set of default [DistributedPlannerExtension] implementations is tried. Right
/// now the only default [DistributedPlannerExtension] is [FileScanConfigDistributedPlannerExtension].
#[derive(Clone, Default)]
pub(crate) struct CombinedDistributedPlannerExtension {
    pub(crate) user_provided: Vec<Arc<dyn DistributedPlannerExtension + Send + Sync>>,
}

impl DistributedPlannerExtension for CombinedDistributedPlannerExtension {
    fn compute_cost(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<ComputeCostClass> {
        for estimator in &self.user_provided {
            if let Some(compute_cost) = estimator.compute_cost(plan, cfg) {
                return Some(compute_cost);
            }
        }
        None
    }

    fn max_tasks(&self, plan: &Arc<dyn ExecutionPlan>, cfg: &ConfigOptions) -> Option<usize> {
        for estimator in &self.user_provided {
            if let Some(max_tasks) = estimator.max_tasks(plan, cfg) {
                return Some(max_tasks);
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
        for default_estimator in
            [&FileScanConfigDistributedPlannerExtension as &dyn DistributedPlannerExtension]
        {
            if let Some(result) = default_estimator.scale_up_leaf_node(plan, task_count, cfg) {
                return Some(result);
            }
        }
        None
    }
}
