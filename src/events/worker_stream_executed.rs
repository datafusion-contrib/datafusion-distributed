use super::common::EventHandlerChain;
use datafusion::execution::config::SessionConfig;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use std::sync::Arc;

use crate::TaskKey;

/// Information supplied when a worker is about to start one partition's stream for a task.
pub struct WorkerStreamExecutedEvent<'a> {
    /// The stream about to be returned to the caller. Each handler receives the stream
    /// returned by the previous handler.
    pub stream: SendableRecordBatchStream,
    /// Which task this stream belongs to, and which partition of it this is.
    pub task_key: TaskKey,
    /// Which partition of the task this stream is for.
    pub partition: usize,
    /// The task context the stream was created with.
    pub task_ctx: &'a Arc<TaskContext>,
}

/// The stream produced by a [`WorkerStreamExecutedHandler`].
pub struct WorkerStreamExecutedEventResponse {
    /// The original or wrapped stream. If wrapped, the wrapper needs to be transparent to the
    /// stream's contents (schema, items, errors) — see [`WorkerStreamExecutedHandler`].
    pub stream: SendableRecordBatchStream,
}

impl WorkerStreamExecutedEventResponse {
    /// Returns a response containing the stream to pass to the next handler, or to actually
    /// execute.
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

/// Wraps a worker's per-partition stream right before it starts being polled.
///
/// Unlike [`crate::TaskFinishedHandler`], which reports a value once a task is done, this
/// handler participates in a task's actual execution: it exists for consumers that need to run
/// code around every poll and the eventual teardown of a task's streams — attributing
/// allocations to an external accounting context, most notably, the same way a consumer's own
/// code might already wrap a *coordinator's* result stream for an equivalent purpose.
///
/// Handlers run in registration order, each receiving the stream returned by the previous
/// handler, the same threading [`crate::WorkerPlanRewriteHandler`] uses for a worker's plan.
/// Unlike [`crate::TaskFinishedHandler`], an error here is not swallowed: it is returned as this
/// partition's stream, the same as any other execution failure.
///
/// A task's partitions are not necessarily all requested together: `target_partition_start..
/// target_partition_end` in one [`crate::ExecuteTaskRequest`] may cover only some of a task's
/// partitions, with the rest requested separately. A handler that needs one piece of state
/// shared across every stream for the same task (e.g. a single accounting handle, registered
/// once and torn down once) must key that state by `task_key` itself, not assume this fires
/// exactly once per task — see [`crate::TaskFinishedHandler`] for the one event that *is*
/// guaranteed to fire exactly once per task, regardless of how many `WorkerStreamExecutedEvent`s
/// preceded it.
///
/// The handler is deliberately **not** `async`: it runs inline, synchronously, right before a
/// partition's stream starts being polled.
pub trait WorkerStreamExecutedHandler: Send + Sync + 'static {
    /// Returns the stream to pass to the next handler, or to actually execute.
    fn handle(
        &self,
        ev: WorkerStreamExecutedEvent<'_>,
    ) -> datafusion::common::Result<WorkerStreamExecutedEventResponse>;
}

impl<F> WorkerStreamExecutedHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(
        WorkerStreamExecutedEvent<'a>,
    ) -> datafusion::common::Result<WorkerStreamExecutedEventResponse>,
{
    fn handle(
        &self,
        ev: WorkerStreamExecutedEvent<'_>,
    ) -> datafusion::common::Result<WorkerStreamExecutedEventResponse> {
        self(ev)
    }
}

impl WorkerStreamExecutedHandler for Arc<dyn WorkerStreamExecutedHandler> {
    fn handle(
        &self,
        ev: WorkerStreamExecutedEvent<'_>,
    ) -> datafusion::common::Result<WorkerStreamExecutedEventResponse> {
        self.as_ref().handle(ev)
    }
}

pub(crate) type WorkerStreamExecutedHandlers = EventHandlerChain<dyn WorkerStreamExecutedHandler>;

impl WorkerStreamExecutedHandlers {
    /// Runs every registered handler in order, each wrapping the stream returned by the
    /// previous one. Returns an error immediately if any handler does, without running the
    /// remaining handlers.
    pub(crate) fn handle(
        stream: SendableRecordBatchStream,
        task_key: TaskKey,
        partition: usize,
        task_ctx: &Arc<TaskContext>,
        session_config: &SessionConfig,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let mut stream = stream;
        if let Some(handlers) = session_config.get_extension::<WorkerStreamExecutedHandlers>() {
            for handler in handlers.iter() {
                stream = handler
                    .handle(WorkerStreamExecutedEvent {
                        stream,
                        task_key,
                        partition,
                        task_ctx,
                    })?
                    .stream;
            }
        }
        Ok(stream)
    }
}
