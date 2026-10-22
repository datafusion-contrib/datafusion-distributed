use super::common::EventHandlerChain;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use std::sync::Arc;

use crate::{DistributedTaskContext, TaskKey};

/// Information supplied when a task finishes executing on a worker.
pub struct TaskFinishedEvent<'a> {
    /// The worker-local plan that was executed.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// This task's full key, including its query id. The same value a
    /// [`crate::WorkerStreamExecutedEvent`] for one of this task's partitions carries, so state
    /// keyed by one can be correlated with, and torn down by, the other.
    pub task_key: TaskKey,
    /// Which task of the stage this is, and how many tasks the stage has.
    pub task_context: DistributedTaskContext,
    /// The configuration of the worker session that executed the plan.
    pub session_config: &'a SessionConfig,
}

/// The task-scoped metrics produced by a [`TaskFinishedHandler`].
pub struct TaskFinishedEventResponse {
    /// Metrics to report for this task as a whole, as opposed to metrics tied to a specific
    /// plan node. These are merged with, not replacing, the metrics every task already reports
    /// (e.g. the timestamps at which the plan was added, started, and finished executing).
    pub metrics: MetricsSet,
}

impl TaskFinishedEventResponse {
    /// Returns a response contributing the given metrics for this task.
    pub fn new(metrics: MetricsSet) -> Self {
        Self { metrics }
    }
}

/// Reports task-scoped metrics when a task finishes executing on a worker.
///
/// Unlike metrics attached to a specific [`ExecutionPlan`] node, these are scoped to the task as
/// a whole — useful for values with no natural single-node owner (e.g. peak memory used by the
/// task, as measured by a process-wide allocator hook).
///
/// Every registered handler runs, and the metrics it returns are appended to this task's
/// metrics. There is no threading of a value between handlers (unlike, for example,
/// [`crate::WorkerPlanRewriteHandler`]): each handler's output is independent and purely
/// additive.
///
/// The handler is deliberately **not** `async`: this runs inline, synchronously, at the point a
/// task's metrics are about to be sent back to the coordinator. Observability must never make a
/// worker wait on I/O, so a handler that needs to await something to produce its value is doing
/// more than observability and does not fit this API — compute and cache the value ahead of
/// time instead, and have the handler just read it.
///
/// A handler returning an error contributes no metrics; the error is not propagated and does
/// not prevent other handlers from running or the task's own metrics from being sent. Metrics
/// are best-effort and must never affect query correctness or success.
///
/// Handlers run every time a task finishes, regardless of whether distributed metric collection
/// (`DistributedConfig.collect_metrics`) is enabled for the query. A handler may rely on this
/// for teardown that must happen exactly once per task even when its own contributed metrics
/// end up being discarded because nothing is listening for them.
pub trait TaskFinishedHandler: Send + Sync + 'static {
    /// Returns the metrics to contribute for this task.
    fn handle(
        &self,
        ev: TaskFinishedEvent<'_>,
    ) -> datafusion::common::Result<TaskFinishedEventResponse>;
}

impl<F> TaskFinishedHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(TaskFinishedEvent<'a>) -> datafusion::common::Result<TaskFinishedEventResponse>,
{
    fn handle(
        &self,
        ev: TaskFinishedEvent<'_>,
    ) -> datafusion::common::Result<TaskFinishedEventResponse> {
        self(ev)
    }
}

impl TaskFinishedHandler for Arc<dyn TaskFinishedHandler> {
    fn handle(
        &self,
        ev: TaskFinishedEvent<'_>,
    ) -> datafusion::common::Result<TaskFinishedEventResponse> {
        self.as_ref().handle(ev)
    }
}

pub(crate) type TaskFinishedHandlers = EventHandlerChain<dyn TaskFinishedHandler>;

impl TaskFinishedHandlers {
    /// Runs every registered handler and returns the concatenation of every metric they
    /// contributed. Handlers that return an error contribute nothing; the error is dropped, not
    /// propagated.
    pub(crate) fn handle(
        plan: &Arc<dyn ExecutionPlan>,
        task_key: TaskKey,
        task_context: DistributedTaskContext,
        session_config: &SessionConfig,
    ) -> MetricsSet {
        let mut metrics = MetricsSet::new();
        let Some(handlers) = session_config.get_extension::<TaskFinishedHandlers>() else {
            return metrics;
        };
        for handler in handlers.iter() {
            let ev = TaskFinishedEvent {
                plan,
                task_key,
                task_context,
                session_config,
            };
            if let Ok(response) = handler.handle(ev) {
                for metric in response.metrics.iter() {
                    metrics.push(Arc::clone(metric));
                }
            }
        }
        metrics
    }
}
