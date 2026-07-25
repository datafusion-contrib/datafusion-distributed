use super::common::EventHandlerChain;
use datafusion::error::Result;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Information supplied while rewriting a decoded worker stage plan before registration.
pub struct WorkerPlanRewriteEvent<'a> {
    /// The worker-local plan. Each handler receives the plan returned by the previous handler.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The configuration of the worker session that will execute the plan.
    pub session_config: &'a SessionConfig,
}

/// The worker-local plan produced by a [`WorkerPlanRewriteHandler`].
pub struct WorkerPlanRewriteEventResponse {
    /// The original or transformed plan. If transformed, the plan needs to maintain the same
    /// topology.
    pub plan: Arc<dyn ExecutionPlan>,
}

impl WorkerPlanRewriteEventResponse {
    /// Returns a response containing the rewritten worker-local plan.
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}

/// Rewrites a decoded worker-local plan before it is registered for execution.
///
/// Every registered handler runs in registration order and receives the plan returned by the
/// previous handler. Returning an error aborts plan registration.
pub trait WorkerPlanRewriteHandler: Send + Sync + 'static {
    /// Returns the plan to pass to the next handler.
    fn rewrite_worker_plan(
        &self,
        ev: WorkerPlanRewriteEvent,
    ) -> Result<WorkerPlanRewriteEventResponse>;
}

impl<F> WorkerPlanRewriteHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(WorkerPlanRewriteEvent<'a>) -> Result<WorkerPlanRewriteEventResponse>,
{
    fn rewrite_worker_plan(
        &self,
        ev: WorkerPlanRewriteEvent,
    ) -> Result<WorkerPlanRewriteEventResponse> {
        self(ev)
    }
}

pub(crate) type WorkerPlanRewriteHandlers = EventHandlerChain<dyn WorkerPlanRewriteHandler>;

impl WorkerPlanRewriteHandlers {
    pub(crate) fn handle(ev: WorkerPlanRewriteEvent) -> Result<WorkerPlanRewriteEventResponse> {
        let WorkerPlanRewriteEvent {
            plan,
            session_config,
        } = ev;
        let plan = match session_config.get_extension::<WorkerPlanRewriteHandlers>() {
            Some(handlers) => handlers.try_fold(plan, |plan, handler| {
                handler
                    .rewrite_worker_plan(WorkerPlanRewriteEvent {
                        plan,
                        session_config,
                    })
                    .map(|response| response.plan)
            })?,
            None => plan,
        };
        Ok(WorkerPlanRewriteEventResponse::new(plan))
    }
}
