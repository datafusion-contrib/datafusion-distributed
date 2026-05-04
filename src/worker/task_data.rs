use crate::worker::generated::worker::PreOrderTaskMetrics;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::oneshot;

#[derive(Clone, Debug)]
/// TaskData stores state for a single task being executed by this Endpoint. It may be shared
/// by concurrent requests for the same task which execute separate partitions.
pub struct TaskData {
    /// Task context suitable for execute different partitions from the same task.
    pub(super) task_ctx: Arc<TaskContext>,
    /// Plan to be executed.
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// `num_partitions_remaining` is initialized to the total number of partitions in the task (not
    /// only tasks in the partition group). This is decremented for each request to the endpoint
    /// for this task. Once this count is zero, the task is likely complete. The task may not be
    /// complete because it's possible that the same partition was retried and this count was
    /// decremented more than once for the same partition.
    pub(super) num_partitions_remaining: Arc<AtomicUsize>,
    /// Sender half of the metrics channel. `impl_execute_task` takes this (via `Option::take`)
    /// once all partitions have finished or been dropped, sending the collected metrics back to
    /// the coordinator through the `CoordinatorChannel` side channel.
    pub(super) metrics_tx: Arc<std::sync::Mutex<Option<oneshot::Sender<PreOrderTaskMetrics>>>>,
}

impl TaskData {
    /// Returns the number of partitions remaining to be processed.
    pub(crate) fn num_partitions_remaining(&self) -> usize {
        self.num_partitions_remaining
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Returns the total number of partitions in this task.
    pub(crate) fn total_partitions(&self) -> usize {
        self.plan.properties().partitioning.partition_count()
    }
}
