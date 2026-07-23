use super::common::EventHandlerChain;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use url::Url;

/// Information supplied when the coordinator assigns a stage's tasks to workers.
///
/// A routing response contains one worker URL per task, in task-index order. The coordinator
/// validates that a response has exactly [`Self::handle`] URLs before it starts the stage.
#[derive(Clone)]
pub struct RouteTasksEvent<'a> {
    /// The task context active for the query being coordinated.
    pub task_ctx: Arc<TaskContext>,
    /// The head execution plan of the stage whose tasks are being routed.
    /// WARNING: this is never going to be a custom leaf node, this is the head node of the fragment
    ///  of the plan that contains the custom leaf node.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// The number of task slots that need a worker assignment.
    pub task_count: usize,
}

/// Worker assignments returned by a [`RouteTasksHandler`].
pub struct RouteTasksEventResponse {
    /// One worker URL per task, ordered by task index.
    pub urls: Vec<Url>,
}

impl RouteTasksEventResponse {
    /// Returns a response assigning tasks to `urls` in order.
    ///
    /// The coordinator rejects the response unless `urls.len()` equals
    /// [`RouteTasksEvent::handle`].
    pub fn new(urls: Vec<Url>) -> Self {
        Self { urls }
    }
}

/// Optionally assigns a stage's task slots to worker URLs.
///
/// Handlers are evaluated in reverse registration order. Return `Some(Ok(_))` to select a
/// complete routing response and stop dispatch, or `None` to defer to earlier handlers. If every
/// handler returns `None`, the coordinator assigns the tasks round-robin, from a randomized worker
/// offset. Returning `Some(Err(_))` aborts execution before tasks are submitted.
pub trait RouteTasksHandler: Send + Sync + 'static {
    /// Optionally assigns the tasks described by `ev` to specific worker URLs.
    ///
    /// Return `None` when this handler does not apply. A successful response must provide exactly
    /// one URL for every task, in task-index order.
    fn handle(&self, ev: RouteTasksEvent) -> Option<Result<RouteTasksEventResponse>>;
}

impl<F> RouteTasksHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(RouteTasksEvent<'a>) -> Option<Result<RouteTasksEventResponse>>,
{
    fn handle(&self, ev: RouteTasksEvent) -> Option<Result<RouteTasksEventResponse>> {
        self(ev)
    }
}

pub(crate) type RouteTasksHandlers = EventHandlerChain<dyn RouteTasksHandler>;

impl RouteTasksHandlers {
    pub(crate) fn handle(ev: RouteTasksEvent) -> Option<Result<RouteTasksEventResponse>> {
        ev.task_ctx
            .session_config()
            .get_extension::<RouteTasksHandlers>()?
            .find_map(|handler| handler.handle(ev.clone()))
    }
}
