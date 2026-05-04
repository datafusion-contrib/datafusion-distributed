use crate::common::OnceLockResult;
use crate::distributed_planner::network_boundary_scale_input;
use crate::worker::generated::worker as pb;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    /// Task context suitable for execute different partitions from the same task.
    pub(super) task_ctx: Arc<TaskContext>,
    pub(crate) base_plan: Arc<dyn ExecutionPlan>,
    pub(crate) scaled_up_plan: Arc<OnceLockResult<Arc<dyn ExecutionPlan>>>,
    /// `num_partitions_remaining` is initialized to the total number of partitions in the task (not
    /// only tasks in the partition group). This is decremented for each request to the endpoint
    /// for this task. Once this count is zero, the task is likely complete. The task may not be
    /// complete because it's possible that the same partition was retried and this count was
    /// decremented more than once for the same partition.
    pub(super) num_partitions_remaining: Arc<AtomicUsize>,
    /// Sender half of the metrics channel. `impl_execute_task` takes this (via `Option::take`)
    /// once all partitions have finished or been dropped, sending the collected metrics back to
    /// the coordinator through the `CoordinatorChannel` side channel.
    pub(super) metrics_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<pb::PreOrderTaskMetrics>>>>,
}

impl TaskData {
    /// Returns the number of partitions remaining to be processed.
    pub(crate) fn num_partitions_remaining(&self) -> usize {
        self.num_partitions_remaining.load(Ordering::SeqCst)
    }

    /// Returns the total number of partitions in this task.
    pub(crate) fn total_partitions(&self) -> usize {
        match self.scaled_up_plan.get() {
            Some(Ok(plan)) => plan.output_partitioning().partition_count(),
            _ => self
                .base_plan
                .properties()
                .output_partitioning()
                .partition_count(),
        }
    }

    pub(crate) fn scaled_up_plan(
        &self,
        consumer_partitions: usize,
        consumer_task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = self.scaled_up_plan.get_or_init(|| {
            let scaled_up = match network_boundary_scale_input(
                Arc::clone(&self.base_plan),
                consumer_partitions,
                consumer_task_count,
            ) {
                Ok(scaled_up) => scaled_up,
                Err(err) => return Err(Arc::new(err)),
            };

            let partition_count = scaled_up.output_partitioning().partition_count();
            self.num_partitions_remaining
                .store(partition_count, Ordering::SeqCst);
            Ok(scaled_up)
        });
        match result {
            Ok(plan) => Ok(Arc::clone(plan)),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}
