use crate::{PartitionIsolatorExec, TaskEstimation, TaskEstimator};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Upon encountering a [DataSourceExec], it wraps it in a [PartitionIsolatorExec] and
/// returns a fixed task count.
pub struct FixedDataSourceExecTaskEstimator(pub usize);

impl TaskEstimator for FixedDataSourceExecTaskEstimator {
    fn estimate_tasks(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        if plan.as_any().is::<DataSourceExec>() {
            Some(TaskEstimation {
                task_count: self.0,
                new_plan: Some(Arc::new(PartitionIsolatorExec::new(Arc::clone(plan)))),
            })
        } else {
            None
        }
    }
}
