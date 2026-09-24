use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::runtime::JoinSet;
use datafusion::common::{Result, exec_datafusion_err};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::{CancellationToken, DropGuard};

/// Owns query task spawning and shutdown independently of planning and worker state.
pub(super) struct QueryTaskState {
    cancel_token: CancellationToken,
    // Owns all the tasks for a query.
    //
    // Mutex + Open are used to take the joinset when the query completes or errors, preventing
    // any new tasks from being spawned.
    join_set: Mutex<Option<JoinSet<Result<()>>>>,
}

impl QueryTaskState {
    pub(super) fn new() -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            join_set: Mutex::new(Some(JoinSet::new())),
        }
    }

    /// Returns a cancellation token that, when cancelled, drops all tasks.
    pub(super) fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    /// Returns a guard that, when dropped, drops all tasks.
    pub(super) fn end_query_guard(&self) -> DropGuard {
        self.cancel_token.clone().drop_guard()
    }

    /// Spawns a task in the joinset to execute the provided future.
    pub(super) fn spawn(&self, task: impl Future<Output = Result<()>> + Send + 'static) {
        if let Some(join_set) = self.join_set.lock().unwrap().as_mut() {
            join_set.spawn(task);
        }
    }

    /// Builds a [`SendableRecordBatchStream`] for the query. The returned stream will
    /// error if any tasks associated with the query lifetime error.
    pub(super) fn output_stream(
        self: Arc<Self>,
        schema: SchemaRef,
        batches: Receiver<RecordBatch>,
    ) -> SendableRecordBatchStream {
        Box::pin(QueryStream {
            task_state: self,
            schema,
            batches,
        })
    }

    /// Cancels the query, dropping every task related to this query.
    fn cancel(&self) {
        self.cancel_token.cancel();
        let tasks = self.join_set.lock().unwrap().take();
        drop(tasks);
    }
}

/// [`RecordBatchStream`] for the entire query which errors if any tasks associated
/// with the query error.
struct QueryStream {
    /// Main query record batch stream.
    batches: Receiver<RecordBatch>,
    schema: SchemaRef,
    /// Owns tasks associated with the query.
    task_state: Arc<QueryTaskState>,
}

impl Stream for QueryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Surface errors in any tasks. 
        let tasks_finished = loop {
            let result = {
                let mut tasks = self.task_state.join_set.lock().unwrap();
                let Some(tasks) = tasks.as_mut() else {
                    return Poll::Ready(None);
                };
                tasks.poll_join_next(cx)
            };
            match result {
                Poll::Ready(Some(Ok(Ok(())))) => continue,
                Poll::Ready(Some(Ok(Err(error)))) => {
                    self.task_state.cancel();
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Err(error))) => {
                    self.task_state.cancel();
                    /// If the child task panicked, then panic on this main task.
                    if error.is_panic() {
                        resume_unwind(error.into_panic());
                    }
                    return Poll::Ready(Some(Err(exec_datafusion_err!(
                        "Non panic JoinSet Error: {error}"
                    ))));
                }
                Poll::Ready(None) => break true,
                Poll::Pending => break false,
            }
        };

        // Poll the main RecordBatch steam.
        match self.batches.poll_recv(cx) {
            Poll::Ready(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(None) if tasks_finished => {
                /// All tasks including the main query RecordBatch stream finished.
                self.task_state.cancel();
                Poll::Ready(None)
            }
            // Wait for all owned tasks to finish. 
            _ => Poll::Pending,
        }
    }
}

impl RecordBatchStream for QueryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Drop for QueryStream {
    fn drop(&mut self) {
        self.task_state.cancel();
    }
}
