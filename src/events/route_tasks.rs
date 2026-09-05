use crate::events::common::EventHandlerChain;
use crate::{TaskKey, WorkerResolver, WorkerToCoordinatorMsg};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::BoxStream;
use std::sync::Arc;
use url::Url;

/// Information supplied when the coordinator assigns one distributed task to a worker.
#[derive(Clone, Copy)]
pub struct RouteTaskEvent<'a> {
    /// The task context active for the query being coordinated.
    pub task_ctx: &'a TaskContext,
    /// [WorkerResolver] in scope.
    pub worker_resolver: &'a dyn WorkerResolver,
    /// Identifier of the task that is getting assigned.
    pub task_key: TaskKey,
    /// Number of tasks of the stage to which the assigned task belongs.
    pub task_count: usize,
    /// The exact plan variant that will be sent for this task.
    pub task_specialized_plan: &'a Arc<dyn ExecutionPlan>,
    /// Establishes a coordinator-to-worker connection for a candidate URL.
    pub dialer: &'a dyn CoordinatorToWorkerDialer,
}

/// A worker connection selected by a [`RouteTaskHandler`].
pub struct RouteTaskEventResponse {
    /// The URL of the selected worker.
    pub url: Url,
    /// Messages streamed from the selected worker to the coordinator.
    pub worker_to_coordinator_stream: BoxStream<'static, Result<WorkerToCoordinatorMsg>>,
}

/// Assigns a distributed task to a worker and establishes its connection.
///
/// Handlers are evaluated in registration order, with custom handlers before built-ins. Return
/// `None` to defer to the next handler. To implement retries or failover, call
/// [`RouteTaskEvent::dialer`] multiple times and return the successful response.
#[async_trait]
pub trait RouteTaskHandler: Send + Sync + 'static {
    /// Attempts to assign the task described by the event to a worker.
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>>;
}

#[async_trait]
impl RouteTaskHandler for Arc<dyn RouteTaskHandler> {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        self.as_ref().handle(ev).await
    }
}

pub(crate) type RouteTaskHandlers = EventHandlerChain<dyn RouteTaskHandler>;

impl RouteTaskHandlers {
    pub(crate) async fn handle(ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let handlers = ev
            .task_ctx
            .session_config()
            .get_extension::<RouteTaskHandlers>()?;
        for handler in handlers.iter() {
            if let Some(response) = handler.handle(ev).await {
                return Some(response);
            }
        }
        None
    }
}

// === CoordinatorToWorkerDialer ===

/// Establishes coordinator-to-worker connections for assignment candidates.
///
/// Each call represents an independent connection attempt. A handler may call `dial` multiple
/// times, sequentially or concurrently, when implementing retries or failover.
#[async_trait]
pub trait CoordinatorToWorkerDialer: Send + Sync {
    /// Attempts to connect the task to `url`.
    async fn dial(&self, url: Url) -> Result<RouteTaskEventResponse>;
}

pub(crate) fn new_coordinator_to_worker_dialer<F, Fut>(f: F) -> impl CoordinatorToWorkerDialer
where
    F: Fn(Url) -> Fut + Send + Sync,
    Fut: Future<Output = Result<RouteTaskEventResponse>> + Send,
{
    struct S<F>(F);

    #[async_trait]
    impl<F, Fut> CoordinatorToWorkerDialer for S<F>
    where
        F: Fn(Url) -> Fut + Send + Sync,
        Fut: Future<Output = Result<RouteTaskEventResponse>> + Send,
    {
        async fn dial(&self, url: Url) -> Result<RouteTaskEventResponse> {
            self.0(url).await
        }
    }

    S(f)
}
