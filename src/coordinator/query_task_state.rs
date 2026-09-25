use datafusion::common::runtime::JoinSet;
use datafusion::common::{Result, exec_err};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use futures::future::BoxFuture;
use futures::{FutureExt, TryStreamExt, stream};
use std::panic::resume_unwind;
use tokio::select;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use tokio_util::sync::{CancellationToken, DropGuard};

/// Owns query task spawning and shutdown independently of planning and worker state.
pub(super) struct QueryTaskState {
    // Marks the end of the end query in both success and failure cases.
    query_finished: CancellationToken,
    task_tx: UnboundedSender<BoxFuture<'static, Result<()>>>,
}

impl QueryTaskState {
    pub(super) fn new(output: &mut RecordBatchReceiverStreamBuilder) -> Self {
        let query_finished = CancellationToken::new();
        let (task_tx, mut task_rx) = unbounded_channel::<BoxFuture<'static, Result<()>>>();
        let guard = query_finished.clone().drop_guard();

        // This task owns all of the tasks associated with the query. If any task errors,
        // it signals that the query is finished and drops all the tasks.
        //
        // Note that owned tasks may outlive the query itself (ex. metrics collection). This
        // task only finishes when those are done. It's expected that tasks have a bounded
        // lifetime or finish gracefully when the query_finish signal is fired.
        output.spawn(async move {
            let _guard = guard;
            let mut tasks = JoinSet::new();
            let mut closed = false;
            loop {
                select! {
                    result = tasks.join_next(), if !tasks.is_empty() => {
                        match result {
                            Some(Ok(result)) => result?,
                            Some(Err(error)) => {
                                if error.is_panic() {
                                    resume_unwind(error.into_panic());
                                }
                                return exec_err!("non panic JoinSet Error: {error}");
                            }
                            None => unreachable!("nonempty JoinSet returned no task"),
                        }
                    }
                    task = task_rx.recv(), if !closed => {
                        match task {
                            Some(task) => { tasks.spawn(task); }
                            None => closed = true,
                        }
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

    /// Returns the token used to close coordinator-to-worker channels.
    pub(super) fn query_finished(&self) -> CancellationToken {
        self.query_finished.clone()
    }

    /// Returns a guard that signals query completion when dropped.
    pub(super) fn end_query_guard(&self) -> DropGuard {
        self.query_finished.clone().drop_guard()
    }

    /// Registers a task to run in the supervisor's JoinSet.
    pub(super) fn spawn(&self, task: impl Future<Output = Result<()>> + Send + 'static) {
        // Once the supervisor stops, sending fails and drops the future without running it.
        let _ = self.task_tx.send(task.boxed());
    }

    pub(super) fn output_stream(
        &self,
        output: RecordBatchReceiverStreamBuilder,
    ) -> SendableRecordBatchStream {
        let output = output.build();
        let schema = output.schema();
        // Construct the guard before polling so dropping an unpolled stream also cancels.
        let state = (output, self.end_query_guard());
        let stream = stream::try_unfold(state, |(mut output, guard)| async move {
            // Stop after the first error, dropping `output` and the guard even if
            // the caller retains this stream.
            Ok(output
                .try_next()
                .await?
                .map(|batch| (batch, (output, guard))))
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}
