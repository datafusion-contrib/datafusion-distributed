use crate::distributed_planner::ProducerHead;
use crate::passthrough_headers::get_passthrough_headers;
use crate::stage::RemoteStage;
use crate::worker::worker_service::TaskDataEntries;
use crate::{
    ChannelResolver, ExecuteTaskRequest, TaskKey, Worker, get_distributed_channel_resolver,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricBuilder;
use futures::future::{BoxFuture, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};
use url::Url;

/// Context set by [crate::Worker::coordinator_channel] in DataFusion's
/// [datafusion::prelude::SessionConfig] that contains information about the local tasks the current
/// [crate::Worker] owns.
///
/// This information can be used for executing tasks locally bypassing gRPC comms if the tasks that
/// needs to be remotely executed happens to be owned by this same worker.
pub(crate) struct LocalWorkerContext {
    /// The registry of in-flight tasks the [crate::Worker] in the current scope owns.
    pub(crate) task_data_entries: Arc<TaskDataEntries>,
    /// The URL of the [crate::Worker] in scope. When trying to reach to a target URL that happens
    /// to be the same as this one, local comms are preferred instead.
    pub(crate) self_url: Url,
}

/// Manages connections to remote workers.
/// - Handles a range of partitions at a time in other to give the chance to [crate::WorkerChannel]
///   implementations to batch RecordBatch streams up, avoiding the overhead of multiple small
///   streams over an IO interface.
/// - Short circuits to a local in-memory connection if the remote worker that should be reached
///   happens to be the same one issuing the request.
/// - Lazy inits connections to a remote worker on first call to [WorkerConnectionPool::execute].
pub(crate) struct WorkerConnectionPool {
    lazy_stream_groups: Vec<OnceLock<SharedBoxFuture<StreamGroup>>>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

/// A list of consumable RecordBatch streams, each one wrapped by `Mutex<Option<_>>` for
/// exactly-once consumption semantics.
type StreamGroup = Arc<Vec<Mutex<Option<BoxStream<'static, Result<RecordBatch>>>>>>;
/// Just some boilerplate for a shared future.
type SharedBoxFuture<T> = Shared<BoxFuture<'static, Result<T, Arc<DataFusionError>>>>;

impl WorkerConnectionPool {
    /// Builds a new [WorkerConnectionPool] with as many empty slots for worker stream entries as
    /// the provided `input_tasks`.
    pub(crate) fn new(input_tasks: usize) -> Self {
        Self {
            lazy_stream_groups: (0..input_tasks).map(|_| OnceLock::new()).collect(),
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }

    pub(crate) fn execute(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        target_partition: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let ch_resolver = get_distributed_channel_resolver(ctx.as_ref());

        let Some(target_url) = input_stage.workers.get(target_task).cloned() else {
            internal_err!("input_stage.workers[{target_task}] out of range.")?
        };
        let task_key = TaskKey {
            query_id: input_stage.query_id,
            stage_id: input_stage.num,
            task_number: target_task,
        };
        let output_bytes = MetricBuilder::new(&self.metrics).output_bytes(target_partition);

        // If we are physically in the same worker, short circuit into a local connection without
        // going through the `WorkerChannel`.
        if let Some(result) = self.local_stream_short_circuit(
            task_key,
            target_partition,
            &target_url,
            &producer_head,
            ctx,
        )? {
            return Ok(result
                .inspect_ok(move |batch| {
                    output_bytes.add(logical_record_batch_size(batch));
                })
                .boxed());
        }

        // Otherwise, we need to reach the remote worker through the `WorkerChannel`. Unlike local
        // connections, these remote connections span a range of partitions so that `WorkerChannel`
        // implementations have the option of batch them.
        let Some(worker_connection) = self.lazy_stream_groups.get(target_task) else {
            return internal_err!(
                "WorkerConnections: Task index {target_task} not found, only have {} tasks",
                self.lazy_stream_groups.len()
            );
        };

        let streams_shared_future = worker_connection.get_or_init(|| {
            let metrics = self.metrics.clone();
            let ctx = Arc::clone(ctx);

            // The relevant entry from `task_data_entries` needs to be eagerly retrieved, it cannot be
            // left for until someone decides to start polling the returned `BoxStream`, otherwise,
            // there's risk that the entry is evicted by Moka's TTL, and by the time the returned stream
            // is polled, the entry might not be there.
            //
            // Note that this does not start polling the returned streams, it just instantiates them.
            let streams_task = SpawnedTask::spawn(async move {
                let request = ExecuteTaskRequest {
                    task_key,
                    target_partition_start: target_partitions.start,
                    target_partition_end: target_partitions.end,
                    producer_head_spec: producer_head.to_spec(ctx.session_config())?,
                };
                let mut client = ch_resolver.get_worker_client_for_url(&target_url).await?;
                let headers = get_passthrough_headers(ctx.session_config());
                let streams = client.execute_task(headers, request, metrics, &ctx).await?;
                Ok(streams)
            });

            async move {
                match streams_task.await {
                    Ok(Ok(v)) => Ok(Arc::new(
                        v.into_iter().map(|v| Mutex::new(Some(v))).collect(),
                    )),
                    Ok(Err(e)) => Err(Arc::new(e)),
                    Err(e) => Err(Arc::new(internal_datafusion_err!(
                        "JoinError instantiating streams {e}"
                    ))),
                }
            }
            .boxed()
            .shared()
        });

        let streams_future = streams_shared_future.clone();
        Ok(async move {
            let streams = streams_future.await.map_err(DataFusionError::Shared)?;
            let Some(slot) = streams.get(target_partition - target_partitions.start) else {
                return internal_err!(
                    "WorkerConnections has no stream for partition {target_partition}. Was it already consumed?"
                );
            };
            slot.lock().unwrap().take().ok_or_else(|| {
                internal_datafusion_err!(
                    "WorkerConnections stream for partition {target_partition} was already consumed"
                )
            })
        }
        .try_flatten_stream()
        .inspect_ok(move |batch| {
            output_bytes.add(logical_record_batch_size(batch));
        })
        .boxed())
    }

    fn local_stream_short_circuit(
        &self,
        task_key: TaskKey,
        target_partition: usize,
        target_url: &Url,
        producer_head: &ProducerHead,
        ctx: &Arc<TaskContext>,
    ) -> Result<Option<BoxStream<'static, Result<RecordBatch>>>> {
        let Some(task_data_entries) = ctx
            .session_config()
            .get_extension::<LocalWorkerContext>()
            .and_then(|lw_ctx| match &lw_ctx.self_url == target_url {
                true => Some(Arc::clone(&lw_ctx.task_data_entries)),
                false => None,
            })
        else {
            return Ok(None);
        };

        MetricBuilder::new(&self.metrics)
            .global_counter("local_connections_used")
            .add(1);
        let request = ExecuteTaskRequest {
            task_key,
            target_partition_start: target_partition,
            target_partition_end: target_partition + 1,
            producer_head_spec: producer_head.to_spec(ctx.session_config())?,
        };
        // The relevant entry from `task_data_entries` needs to be eagerly retrieved, it cannot be
        // left for until someone decides to start polling the returned `BoxStream`, otherwise,
        // there's risk that the entry is evicted by Moka's TTL, and by the time the returned stream
        // is polled, the entry might not be there.
        //
        // Note that this does not start polling the returned streams, it just instantiates them.
        let stream_task = SpawnedTask::spawn(async move {
            let (mut streams, _) = Worker::execute_task_static(task_data_entries, request).await?;
            if streams.len() != 1 {
                return internal_err!(
                    "Expected exactly 1 stream out of Worker::execute_task_static, but got {}",
                    streams.len()
                );
            }
            Ok::<_, DataFusionError>(streams.swap_remove(0))
        });

        Ok(Some(
            async move {
                stream_task
                    .await
                    .map_err(|err| internal_datafusion_err!("{err}"))?
            }
            .try_flatten_stream()
            .boxed(),
        ))
    }
}

/// Returns the logical size of a batch's slices, excluding unused backing-buffer capacity.
fn logical_record_batch_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|column| column.to_data().get_slice_memory_size().unwrap_or(0))
        .sum()
}

impl Debug for WorkerConnectionPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnections")
            .field("num_connections", &self.lazy_stream_groups.len())
            .finish()
    }
}

impl Clone for WorkerConnectionPool {
    fn clone(&self) -> Self {
        Self::new(self.lazy_stream_groups.len())
    }
}
