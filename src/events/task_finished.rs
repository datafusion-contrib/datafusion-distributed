use super::common::EventHandlerChain;
use datafusion::error::DataFusionError;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use std::sync::Arc;

use crate::{DistributedTaskContext, TaskKey};

/// Information supplied when a task finishes executing on a worker.
pub struct TaskFinishedEvent<'a> {
    /// The worker-local plan that was executed.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// This task's full key, including its query id.
    pub task_key: TaskKey,
    /// Which task of the stage this is, and how many tasks the stage has.
    pub task_context: DistributedTaskContext,
    /// The configuration of the worker session that executed the plan.
    pub session_config: &'a SessionConfig,
    /// How the task ended.
    pub outcome: TaskOutcome<'a>,
}

/// How a task ended on its worker.
#[non_exhaustive]
#[derive(Debug, Clone, Copy)]
pub enum TaskOutcome<'a> {
    /// Every partition stream reached its end.
    Succeeded,
    /// The first error returned by a partition stream. It may come from another task, through a
    /// network boundary.
    Failed(&'a DataFusionError),
    /// Not every partition stream reached its end, and none returned an error. This happens, for
    /// example, when a `LIMIT` is satisfied or another task fails. The worker cannot tell these
    /// apart.
    Incomplete,
}

/// The task-scoped metrics produced by a [`TaskFinishedHandler`].
pub struct TaskFinishedEventResponse {
    /// Metrics for the task as a whole, appended to the task's built-in metrics.
    pub metrics: MetricsSet,
}

impl TaskFinishedEventResponse {
    /// Returns a response contributing the given metrics for this task.
    pub fn new(metrics: MetricsSet) -> Self {
        Self { metrics }
    }
}

/// Runs once per task, when the task finishes on a worker.
///
/// A task finishes when the coordinator closes its channel to the worker. A task whose plan could
/// not be set up produces no event. Handlers run whether or not
/// `DistributedConfig.collect_metrics` is enabled.
///
/// Every registered handler runs, and the metrics each one returns are appended to the task's
/// metrics. Returning an error contributes no metrics and affects neither the task nor the other
/// handlers.
///
/// The handler is not `async`: it runs inline as the task finishes, so it must not wait on I/O.
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
    pub(crate) fn handle(
        plan: &Arc<dyn ExecutionPlan>,
        task_key: TaskKey,
        task_context: DistributedTaskContext,
        session_config: &SessionConfig,
        outcome: TaskOutcome<'_>,
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
                outcome,
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
