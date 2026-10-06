use datafusion::common::runtime::JoinSet;
use datafusion::common::{Result, exec_err};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use futures::FutureExt;
use futures::future::BoxFuture;
use std::panic::resume_unwind;
use tokio::select;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_util::sync::{CancellationToken, DropGuard};

/// Owns query tokio task spawning and error propagation.
pub(super) struct Spawner {
    // Cancelled when the main execution future completes or is dropped.
    query_finished: CancellationToken,
    task_tx: UnboundedSender<BoxFuture<'static, Result<()>>>,
}

impl Spawner {
    pub(super) fn new(output: &mut RecordBatchReceiverStreamBuilder) -> Self {
        let query_finished = CancellationToken::new();
        let (task_tx, mut task_rx) = unbounded_channel::<BoxFuture<'static, Result<()>>>();

        // Supervisor task responsible for propagating errors from tasks to the main
        // query response stream. Terminates when all tasks finished, otherwise
        // blocks the response stream from finishing.
        output.spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                select! {
                    Some(result) = tasks.join_next() => {
                        match result {
                            Ok(result) => result?,
                            Err(error) if error.is_panic() => {
                                resume_unwind(error.into_panic());
                            }
                            Err(error) => {
                                return exec_err!("non panic JoinSet Error: {error}");
                            }
                        }
                    }
                    Some(task) = task_rx.recv() => {
                        tasks.spawn(task);
                    }
                    else => break,
                }
            }
            Ok(())
        });

        Self {
            query_finished,
            task_tx,
        }
    }

    /// Returns the completion signal that tasks may use to abort.
    pub(super) fn query_finished(&self) -> CancellationToken {
        self.query_finished.clone()
    }

    /// Returns a guard that signals query completion when dropped.
    pub(super) fn end_query_guard(&self) -> DropGuard {
        self.query_finished.clone().drop_guard()
    }

    /// Registers a query-scoped task whose errors propagate to the output stream.
    ///
    /// Tasks may choose to abort themselves when the query stream finishes using
    /// [`Spawner::end_query_guard`] or [`Spawner::query_finished`].
    ///
    /// Otherwise, they may choose to terminate gracefully, blocking the response stream.
    pub(super) fn spawn(&self, task: impl Future<Output = Result<()>> + Send + 'static) {
        // Once the supervisor stops, sending fails and drops the future without running it.
        let _ = self.task_tx.send(task.boxed());
    }

    /// Spawns a task whose lifetime is not tied to the response stream and whose
    /// errors do not propagate to the reposne stream.
    pub(super) fn spawn_unbounded(&self, task: impl Future<Output = ()> + Send + 'static) {
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(task);
    }
}
