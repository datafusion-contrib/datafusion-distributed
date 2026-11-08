use super::common::EventHandlerChain;
use async_trait::async_trait;
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
///
/// The handler is `async` so implementations can perform setup work that needs to make network
/// or I/O calls before the plan executes.
#[async_trait]
pub trait WorkerPlanRewriteHandler: Send + Sync + 'static {
    /// Returns the plan to pass to the next handler.
    async fn handle(
        &self,
        ev: WorkerPlanRewriteEvent<'_>,
    ) -> Result<WorkerPlanRewriteEventResponse>;
}

#[async_trait]
impl<F> WorkerPlanRewriteHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(WorkerPlanRewriteEvent<'a>) -> Result<WorkerPlanRewriteEventResponse>,
{
    async fn handle(
        &self,
        ev: WorkerPlanRewriteEvent<'_>,
    ) -> Result<WorkerPlanRewriteEventResponse> {
        self(ev)
    }
}

#[async_trait]
impl WorkerPlanRewriteHandler for Arc<dyn WorkerPlanRewriteHandler> {
    async fn handle(
        &self,
        ev: WorkerPlanRewriteEvent<'_>,
    ) -> Result<WorkerPlanRewriteEventResponse> {
        self.as_ref().handle(ev).await
    }
}

pub(crate) type WorkerPlanRewriteHandlers = EventHandlerChain<dyn WorkerPlanRewriteHandler>;

impl WorkerPlanRewriteHandlers {
    pub(crate) async fn handle(
        ev: WorkerPlanRewriteEvent<'_>,
    ) -> Result<WorkerPlanRewriteEventResponse> {
        let WorkerPlanRewriteEvent {
            mut plan,
            session_config,
        } = ev;
        if let Some(handlers) = session_config.get_extension::<WorkerPlanRewriteHandlers>() {
            for handler in handlers.iter() {
                plan = handler
                    .handle(WorkerPlanRewriteEvent {
                        plan,
                        session_config,
                    })
                    .await?
                    .plan;
            }
        }
        Ok(WorkerPlanRewriteEventResponse::new(plan))
    }
}
