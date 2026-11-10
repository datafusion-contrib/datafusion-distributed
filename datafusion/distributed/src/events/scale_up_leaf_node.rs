use super::common::EventHandlerChain;
use datafusion::error::Result;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Information supplied when a leaf has been assigned its final stage task count.
///
/// This event runs after desired task counts from all of the stage's leaves have been reconciled,
/// so [`Self::handle`] is the count that the stage will actually use. A handler can use it to
/// replace a leaf with a task-specialized plan, such as a [`crate::DistributedLeafExec`] that
/// selects a different input for every task.
#[derive(Clone, Copy)]
pub struct ScaleUpLeafNodeEvent<'a> {
    /// The leaf execution plan to replace, if this handler recognizes it.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// The final number of tasks that will execute the leaf's stage.
    pub task_count: usize,
    /// The session configuration that registered the event handlers and holds query options.
    pub session_config: &'a SessionConfig,
}

/// A replacement plan returned by a [`ScaleUpLeafNodeHandler`].
///
/// The distributed planner annotates every node in this plan with the final stage task count, so
/// the replacement may be a small subtree rather than only a single leaf node.
pub struct ScaleUpLeafNodeEventResponse {
    /// The replacement for [`ScaleUpLeafNodeEvent::plan`].
    pub plan: Arc<dyn ExecutionPlan>,
}

impl ScaleUpLeafNodeEventResponse {
    /// Returns a response that replaces the event's leaf with `plan`.
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}

/// Handles optional leaf rewrites after a stage's task count is final.
///
/// Handlers are evaluated in reverse registration order. Return `Ok(Some(_))` to select a
/// replacement and stop dispatch, or `Ok(None)` to let earlier handlers try the same leaf. If all
/// handlers return `None`, the original leaf is left unchanged. Returning an error aborts planning.
pub trait ScaleUpLeafNodeHandler: Send + Sync + 'static {
    /// Optionally replaces the leaf described by `ev`.
    ///
    /// `ev.task_count` already accounts for the constraints and desired counts of every leaf in
    /// the stage. Implementations should use it when creating the per-task variants of the
    /// replacement plan.
    fn handle(&self, ev: ScaleUpLeafNodeEvent) -> Option<Result<ScaleUpLeafNodeEventResponse>>;
}

impl<F> ScaleUpLeafNodeHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(ScaleUpLeafNodeEvent<'a>) -> Option<Result<ScaleUpLeafNodeEventResponse>>,
{
    fn handle(&self, ev: ScaleUpLeafNodeEvent) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
        self(ev)
    }
}

impl ScaleUpLeafNodeHandler for Arc<dyn ScaleUpLeafNodeHandler> {
    fn handle(&self, ev: ScaleUpLeafNodeEvent) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
        self.as_ref().handle(ev)
    }
}

pub(crate) type ScaleUpLeafNodeHandlers = EventHandlerChain<dyn ScaleUpLeafNodeHandler>;

impl ScaleUpLeafNodeHandlers {
    pub(crate) fn handle(ev: ScaleUpLeafNodeEvent) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
        ev.session_config
            .get_extension::<ScaleUpLeafNodeHandlers>()?
            .find_map(|handler| handler.handle(ev))
    }
}
