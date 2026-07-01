use crate::common::OnceLockResult;
use crate::distributed_planner::ProducerHead;
use crate::passthrough_headers::get_passthrough_headers;
use crate::stage::RemoteStage;
use crate::worker::worker_service::TaskDataEntries;
use crate::{
    ChannelResolver, ExecuteTaskRequest, TaskKey, Worker, get_distributed_channel_resolver,
};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{
    DataFusionError, Result, exec_datafusion_err, internal_datafusion_err, internal_err,
};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricBuilder;
use futures::future::{BoxFuture, Shared};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt};
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

/// Holds lazily initialized partition streams. Each position in the underlying
/// `connections` vector corresponds to the connection to one worker. It assumes a 1:1 mapping
/// between worker and tasks, and upon calling [WorkerConnectionPool::execute]
/// it will initialize the corresponding position in the vector matching the provided `target_task`
/// index.
pub(crate) struct WorkerConnectionPool {
    connections: Vec<OnceLockResult<SharedPartitionStreamsFuture>>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

type PartitionStreams = Arc<Vec<Mutex<Option<BoxStream<'static, Result<RecordBatch>>>>>>;
type SharedPartitionStreamsFuture =
    Shared<BoxFuture<'static, Result<PartitionStreams, Arc<DataFusionError>>>>;

impl WorkerConnectionPool {
    /// Builds a new [WorkerConnectionPool] with as many empty slots for worker stream entries as
    /// the provided `input_tasks`.
    pub(crate) fn new(input_tasks: usize) -> Self {
        let mut connections = Vec::with_capacity(input_tasks);
        for _ in 0..input_tasks {
            connections.push(OnceLock::new());
        }
        Self {
            connections,
            metrics: ExecutionPlanMetricsSet::default(),
        }
    }

    /// Lazily initializes the partition streams corresponding to the provided `target_task`, and
    /// returns the stream for `target_partition`.
    pub(crate) fn execute(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        target_partition: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let Some(worker_connection) = self.connections.get(target_task) else {
            return internal_err!(
                "WorkerConnections: Task index {target_task} not found, only have {} tasks",
                self.connections.len()
            );
        };

        let streams_shared_future = worker_connection.get_or_init(|| {
            let ch_resolver = get_distributed_channel_resolver(ctx.as_ref());

            let Some(target_url) = input_stage.workers.get(target_task).cloned() else {
                internal_err!("input_stage.workers[{target_task}] out of range.")?
            };
            let local_task_data_entries = ctx
                .session_config()
                .get_extension::<LocalWorkerContext>()
                .and_then(|lw_ctx| match lw_ctx.self_url == target_url {
                    true => Some(Arc::clone(&lw_ctx.task_data_entries)),
                    false => None,
                });
            if local_task_data_entries.is_some() {
                MetricBuilder::new(&self.metrics)
                    .global_counter("local_connections_used")
                    .add(1);
            }

            let task_key = TaskKey {
                query_id: input_stage.query_id,
                stage_id: input_stage.num,
                task_number: target_task,
            };

            let request = ExecuteTaskRequest {
                task_key,
                target_partition_start: target_partitions.start,
                target_partition_end: target_partitions.end,
                producer_head_spec: producer_head.to_spec(ctx.session_config())?,
            };
            let metrics = self.metrics.clone();
            let ctx = Arc::clone(ctx);

            // The relevant entry from `task_data_entries` needs to be eagerly retrieved, it cannot be
            // left for until someone decides to start polling the returned `BoxStream`, otherwise,
            // there's risk that the entry is evicted by Moka's TTL, and by the time the returned stream
            // is polled, the entry might not be there.
            //
            // Note that this does not start polling the returned streams, it just instantiates them.
            let streams_task = SpawnedTask::spawn(async move {
                if let Some(task_data_entries) = local_task_data_entries {
                    let (streams, _) =
                        Worker::execute_task_static(task_data_entries, request).await?;
                    let streams = streams.into_iter().map(|v| v.boxed()).collect();
                    Ok::<_, DataFusionError>(streams)
                } else {
                    let mut client = ch_resolver.get_worker_client_for_url(&target_url).await?;
                    let headers = get_passthrough_headers(ctx.session_config());
                    let streams = client.execute_task(headers, request, metrics, &ctx).await?;
                    Ok(streams)
                }
            });

            Ok(async move {
                match streams_task.await {
                    Ok(Ok(v)) => Ok(Arc::new(
                        v.into_iter().map(|v| Mutex::new(Some(v))).collect(),
                    )),
                    Ok(Err(e)) => Err(Arc::new(e)),
                    Err(e) => Err(Arc::new(exec_datafusion_err!(
                        "JoinError instantiating streams {e}"
                    ))),
                }
            }
            .boxed()
            .shared())
        });

        let streams_future = match streams_shared_future {
            Ok(v) => v.clone(),
            Err(err) => return Err(DataFusionError::Shared(Arc::clone(err))),
        };

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
        .boxed())
    }
}

impl Debug for WorkerConnectionPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnections")
            .field("num_connections", &self.connections.len())
            .finish()
    }
}

impl Clone for WorkerConnectionPool {
    fn clone(&self) -> Self {
        Self::new(self.connections.len())
    }
}
