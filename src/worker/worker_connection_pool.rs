use crate::common::OnceLockResult;
use crate::distributed_planner::ProducerHead;
use crate::networking::get_distributed_worker_transport;
use crate::stage::RemoteStage;
use crate::worker::transport::WorkerConnection;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::{Arc, OnceLock};

#[cfg(feature = "flight")]
use crate::common::{on_drop_stream, serialize_uuid};
#[cfg(feature = "flight")]
use crate::metrics::LatencyMetricExt;
#[cfg(feature = "flight")]
use crate::networking::get_distributed_channel_resolver;
#[cfg(feature = "flight")]
use crate::passthrough_headers::get_passthrough_headers;
#[cfg(feature = "flight")]
use crate::protobuf::{datafusion_error_to_tonic_status, map_flight_to_datafusion_error};
#[cfg(feature = "flight")]
use crate::worker::generated::worker::FlightAppMetadata;
#[cfg(feature = "flight")]
use crate::worker::generated::worker::{ExecuteTaskRequest, TaskKey};
#[cfg(feature = "flight")]
use crate::worker::impl_execute_task::execute_local_task;
#[cfg(feature = "flight")]
use crate::worker::worker_service::TaskDataEntries;
#[cfg(feature = "flight")]
use crate::{BytesMetricExt, ChannelResolver, DistributedConfig};
#[cfg(feature = "flight")]
use arrow_flight::FlightData;
#[cfg(feature = "flight")]
use arrow_flight::decode::FlightRecordBatchStream;
#[cfg(feature = "flight")]
use arrow_flight::error::FlightError;
#[cfg(feature = "flight")]
use dashmap::DashMap;
#[cfg(feature = "flight")]
use datafusion::arrow::array::RecordBatch;
#[cfg(feature = "flight")]
use datafusion::common::instant::Instant;
#[cfg(feature = "flight")]
use datafusion::common::runtime::SpawnedTask;
#[cfg(feature = "flight")]
use datafusion::common::{exec_err, internal_datafusion_err};
#[cfg(feature = "flight")]
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
#[cfg(feature = "flight")]
use datafusion::physical_expr_common::metrics::MetricValue;
#[cfg(feature = "flight")]
use datafusion::physical_plan::metrics::{MetricBuilder, Time};
#[cfg(feature = "flight")]
use futures::stream::BoxStream;
#[cfg(feature = "flight")]
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
#[cfg(feature = "flight")]
use http::Extensions;
#[cfg(feature = "flight")]
use pin_project::{pin_project, pinned_drop};
#[cfg(feature = "flight")]
use prost::Message;
#[cfg(feature = "flight")]
use std::borrow::Cow;
#[cfg(feature = "flight")]
use std::pin::Pin;
#[cfg(feature = "flight")]
use std::sync::Mutex;
#[cfg(feature = "flight")]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "flight")]
use std::task::{Context, Poll};
#[cfg(feature = "flight")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(feature = "flight")]
use tokio::sync::Notify;
#[cfg(feature = "flight")]
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
#[cfg(feature = "flight")]
use tokio_stream::wrappers::UnboundedReceiverStream;
#[cfg(feature = "flight")]
use tokio_util::sync::CancellationToken;
#[cfg(feature = "flight")]
use tonic::metadata::MetadataMap;
#[cfg(feature = "flight")]
use tonic::{Request, Status};
#[cfg(feature = "flight")]
use url::Url;

/// Context set by [crate::Worker::coordinator_channel] in DataFusion's
/// [datafusion::prelude::SessionConfig] that contains information about the local tasks the current
/// [crate::Worker] owns.
///
/// This information can be used for executing tasks locally bypassing gRPC comms if the tasks that
/// needs to be remotely executed happens to be owned by this same worker.
#[cfg(feature = "flight")]
pub(crate) struct LocalWorkerContext {
    /// The registry of in-flight tasks the [crate::Worker] in the current scope owns.
    pub(crate) task_data_entries: Arc<TaskDataEntries>,
    /// The URL of the [crate::Worker] in scope. When trying to reach to a target URL that happens
    /// to be the same as this one, local comms are preferred instead.
    pub(crate) self_url: Url,
}

/// Holds a list of lazily initialized [WorkerConnection]s. Each position in the underlying
/// `connections` vector corresponds to the connection to one worker. It assumes a 1:1 mapping
/// between worker and tasks, and upon calling [WorkerConnectionPool::get_or_init_worker_connection]
/// it will initialize the corresponding position in the vector matching the provided `target_task`
/// index.
pub(crate) struct WorkerConnectionPool {
    connections: Vec<OnceLockResult<Box<dyn WorkerConnection>>>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

impl WorkerConnectionPool {
    /// Builds a new [WorkerConnectionPool] with as many empty slots for [WorkerConnection]s as
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

    /// Lazily initializes the [WorkerConnection] corresponding to the provided `target_task`
    /// (therefore maintaining one independent [WorkerConnection] per `target_task`), and
    /// returns it.
    pub(crate) fn get_or_init_worker_connection(
        &self,
        input_stage: &RemoteStage,
        target_partitions: Range<usize>,
        target_task: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
    ) -> Result<&dyn WorkerConnection> {
        let Some(worker_connection) = self.connections.get(target_task) else {
            return internal_err!(
                "WorkerConnections: Task index {target_task} not found, only have {} tasks",
                self.connections.len()
            );
        };

        let conn = worker_connection.get_or_init(|| {
            // Local-vs-remote is the transport's concern now; the pool only caches one connection
            // per worker task and hands the routing decision to the configured transport.
            get_distributed_worker_transport(ctx.session_config())
                .open(
                    input_stage,
                    target_partitions,
                    target_task,
                    producer_head,
                    ctx,
                    &self.metrics,
                )
                .map_err(Arc::new)
        });

        match conn {
            Ok(v) => Ok(v.as_ref()),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}

#[cfg(feature = "flight")]
type WorkerMsg = Result<(FlightData, FlightAppMetadata), Status>;

/// Represents a connection to one [Worker]. Network boundaries will use this for streaming
/// data from single partitions while the actual network communication is handling all the partitions
/// under the hood.
///
/// This is done so that, rather than issuing one gRPC stream per partition, we issue one gRPC stream
/// per group of partitions, and we multiplex streamed record batches locally to in-memory channels.
///
/// Even if Tonic can perfectly multiplex and interleave messages from different gRPC streams through
/// the same underlying TCP connection, there do is some overhead in having one gRPC stream per
/// partition VS a single gRPC stream interleaving multiple partitions. The whole serialized plan
/// needs to be sent over the wire on every gRPC call, so the less gRPC calls we do the better.
#[cfg(feature = "flight")]
pub(crate) struct RemoteWorkerConnection {
    task: Arc<SpawnedTask<()>>,
    not_consumed_streams: Arc<AtomicUsize>,
    cancel_token: CancellationToken,
    per_partition_rx: DashMap<usize, UnboundedReceiver<WorkerMsg>>,

    first_poll_notify: Arc<Notify>,
    // Signals the demux task that buffered memory has been freed by a consumer.
    mem_available_notify: Arc<Notify>,

    // Metrics collection stuff.
    memory_reservation: Arc<MemoryReservation>,
    elapsed_compute: Time,
}

#[cfg(feature = "flight")]
impl RemoteWorkerConnection {
    pub(crate) fn init(
        input_stage: &RemoteStage,
        target_partition_range: Range<usize>,
        target_task: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());
        let buffer_budget_bytes =
            DistributedConfig::from_config_options(ctx.session_config().options())?
                .worker_connection_buffer_budget_bytes;
        // We are retaining record batches in memory until they are consumed, so we need to account
        // for them in the memory pool.
        let memory_reservation =
            Arc::new(MemoryConsumer::new("WorkerConnection").register(ctx.memory_pool()));
        let memory_reservation_clone = Arc::clone(&memory_reservation);

        // Track the maximum memory used to buffer recieved messages.
        let mut curr_max_mem = 0;
        let max_mem_used = MetricBuilder::new(metrics).global_gauge("max_mem_used");
        // Track the total encoded size of all recieved messages.
        let bytes_transferred = MetricBuilder::new(metrics).bytes_counter("bytes_transferred");
        let msg_count = MetricBuilder::new(metrics).global_counter("msg_count");
        // Track end-to-end network latency distribution for all messages.
        let min_latency = MetricBuilder::new(metrics).min_latency("network_latency_min");
        let max_latency = MetricBuilder::new(metrics).max_latency("network_latency_max");
        let p50_latency = MetricBuilder::new(metrics).p50_latency("network_latency_p50");
        let p95_latency = MetricBuilder::new(metrics).p95_latency("network_latency_p95");
        let first_latency = MetricBuilder::new(metrics).first_latency("network_latency_first");
        let sum_latency = Time::new();
        MetricBuilder::new(metrics).build(MetricValue::Time {
            name: Cow::Borrowed("network_latency_sum"),
            time: sum_latency.clone(),
        });
        let latency_count = MetricBuilder::new(metrics).counter("network_latency_count", 0);
        // Track the total CPU time spent in polling messages over the network + decoding them.
        let elapsed_compute = Time::new();
        let elapsed_compute_clone = elapsed_compute.clone();
        MetricBuilder::new(metrics).build(MetricValue::ElapsedCompute(elapsed_compute.clone()));

        // Building the actual request that will be sent to the worker.
        let headers = get_passthrough_headers(ctx.session_config());
        let request = Request::from_parts(
            MetadataMap::from_headers(headers),
            Extensions::default(),
            ExecuteTaskRequest {
                target_partition_start: target_partition_range.start as u64,
                target_partition_end: target_partition_range.end as u64,
                task_key: Some(TaskKey {
                    query_id: serialize_uuid(&input_stage.query_id),
                    stage_id: input_stage.num as u64,
                    task_number: target_task as u64,
                }),
                producer_head: Some(producer_head.to_proto(ctx)?),
            },
        );

        let Some(url) = input_stage.workers.get(target_task).cloned() else {
            return internal_err!("ProgrammingError: Task {target_task} not found");
        };

        // The senders and receivers are unbounded queues used for multiplexing the record
        // batches sent through the single gRPC stream into one stream per partition. They
        // are unbounded to avoid head-of-line blocking: a single bounded queue could block
        // the demux task and starve all sibling partitions even though they have capacity,
        // which deadlocks queries with cross-partition dependencies.
        // Total memory is bounded globally below via `mem_available_notify`.
        let mut per_partition_tx = Vec::with_capacity(target_partition_range.len());
        let per_partition_rx = DashMap::with_capacity(target_partition_range.len());
        for partition in target_partition_range.clone() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WorkerMsg>();
            per_partition_tx.push(tx);
            per_partition_rx.insert(partition, rx);
        }

        let mem_available_notify = Arc::new(Notify::new());
        let mem_available_notify_for_task = Arc::clone(&mem_available_notify);

        let first_poll_notify = Arc::new(Notify::new());
        let first_poll_notify_for_task = Arc::clone(&first_poll_notify);

        // Cancellation token allows us to stop the background task promptly when all partition
        // streams are dropped (e.g., when the query is cancelled).
        let cancel_token = CancellationToken::new();
        let cancel = cancel_token.clone();

        // This task will pull data from all the partitions in `target_partition_range`, and will
        // fan them out to the appropriate `per_partition_rx` based on the "partition" declared
        // in each individual record batch flight metadata.
        let task = SpawnedTask::spawn(async move {
            let mut client = match channel_resolver.get_worker_client_for_url(&url).await {
                Ok(v) => v,
                Err(err) => return fanout(&per_partition_tx, datafusion_error_to_tonic_status(&err))
            };

            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    // If all SendableRecordBatchStreams canceled before any poll, we need to
                    // anyway trigger the task execution and cancel it immediately so that the
                    // cancellation is propagated also in the remote worker. Otherwise, it might
                    // hang forever waiting for someone to execute it.
                    let _ = client.execute_task(request).await;
                    return
                },
                _ = first_poll_notify_for_task.notified() => {}
            }

            let mut interleaved_stream = match client.execute_task(request).await {
                Ok(v) => v.into_inner(),
                Err(err) => return fanout(&per_partition_tx, err),
            };

            loop {
                // Backpressure gate. Per-partition channels are unbounded, so we cap
                // total in-flight buffered bytes here by pausing the gRPC pull when
                // consumers haven't drained enough. This propagates flow control all
                // the way back to the worker without coupling sibling partitions.
                // We always allow a message through when reservation == 0 to avoid
                // livelock if a single message is larger than the budget.
                while memory_reservation.size() >= buffer_budget_bytes {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        _ = mem_available_notify_for_task.notified() => {}
                    }
                }

                // Check for cancellation while waiting for the next message.
                let flight_data = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    msg = interleaved_stream.next() => {
                        match msg {
                            Some(Ok(v)) => v,
                            Some(Err(err)) => return fanout(&per_partition_tx, err),
                            None => return, // Stream exhausted
                        }
                    }
                };

                // Earliest time at which the msg was received.
                let msg_received_time = SystemTime::now();

                let flight_metadata = match FlightAppMetadata::decode(flight_data.app_metadata.as_ref()) {
                    Ok(v) => v,
                    Err(err) => {
                        return fanout(&per_partition_tx, Status::internal(err.to_string()));
                    }
                };

                // Update the running latency tracker.
                let sent_time = UNIX_EPOCH + Duration::from_nanos(flight_metadata.created_timestamp_unix_nanos);
                if flight_metadata.created_timestamp_unix_nanos > 0
                    && let Ok(delta) = msg_received_time.duration_since(sent_time) {
                    min_latency.add_duration(delta);
                    max_latency.add_duration(delta);
                    p50_latency.add_duration(delta);
                    p95_latency.add_duration(delta);
                    first_latency.add_duration(delta);
                    sum_latency.add_duration(delta);
                    latency_count.add(1);
                }

                let partition = flight_metadata.partition as usize;
                // the `per_partition_tx` variable is using a normal `Vec` for storing the
                // channel transmitters, so we need to subtract the `target_partition_range.start`
                // to the `partition` in order to offset it to the appropriate index.
                let sender_i = partition - target_partition_range.start;

                let Some(o_tx) = per_partition_tx.get(sender_i) else {
                    let msg = format!(
                        "Received partition {partition} in Flight metadata, but available partitions are {target_partition_range:?}"
                    );
                    return fanout(&per_partition_tx, Status::internal(msg));
                };

                // We need to send the memory reservation in the same tuple as the actual message
                // so that it gets dropped as soon as the message leaves the queue. Dropping the
                // memory reservation means releasing the memory from the pool for that specific
                // message
                let size = flight_data.encoded_len();
                memory_reservation.grow(size);

                // Update memory related metrics.
                msg_count.add(1);
                bytes_transferred.add_bytes(size);
                let curr_mem = memory_reservation.size();
                if curr_mem > curr_max_mem {
                    curr_max_mem = curr_mem;
                    max_mem_used.set(curr_max_mem);
                }

                if o_tx.send(Ok((flight_data, flight_metadata))).is_err() {
                    // The receiver for this partition was dropped (e.g. a hash join partition
                    // completed early without consuming its probe side). Don't exit: other
                    // partitions multiplexed over the same gRPC stream still need their data.
                    // Undo the memory reservation that was grown for this dropped batch.
                    memory_reservation.shrink(size);
                    continue;
                };
            }
        }.with_elapsed_compute(elapsed_compute));

        Ok(Self {
            task: Arc::new(task),
            cancel_token,
            not_consumed_streams: Arc::new(AtomicUsize::new(per_partition_rx.len())),
            per_partition_rx,
            mem_available_notify,
            first_poll_notify,

            // metrics stuff
            memory_reservation: memory_reservation_clone,
            elapsed_compute: elapsed_compute_clone,
        })
    }
}

#[cfg(feature = "flight")]
impl WorkerConnection for RemoteWorkerConnection {
    /// Streams the provided `partition` from the remote worker.
    ///
    /// This method does not handle any network connection. Instead, the network comms are delegated
    /// to the task spawned by [WorkerConnection::init], who is in charge of polling data not only
    /// from the requested `partition`, but from any other partition in `target_partition_range`.
    /// This method just streams all the record batches belonging to the provided `partition` from
    /// an in-memory queue.
    ///
    /// The task that polls data over the network is held inactive until the first poll to the
    /// stream returned by this method.
    ///
    /// When the returned stream is dropped (e.g., due to query cancellation), the background task
    /// pulling from the Flight stream will be canceled promptly.
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let Some((_, partition_receiver)) = self.per_partition_rx.remove(&partition) else {
            return internal_err!(
                "WorkerConnection has no stream for target partition {partition}. Was it already consumed?"
            );
        };
        let task = Arc::clone(&self.task);
        let cancel_token = self.cancel_token.clone();

        let first_poll_notify = Arc::clone(&self.first_poll_notify);
        let stream = async move {
            first_poll_notify.notify_one();
            UnboundedReceiverStream::new(partition_receiver)
        }
        .flatten_stream();

        let stream = stream.map_err(|err| FlightError::Tonic(Box::new(err)));
        let reservation = Arc::clone(&self.memory_reservation);
        let mem_available_notify = Arc::clone(&self.mem_available_notify);
        let stream = stream.map_ok(move |(data, _meta)| {
            reservation.shrink(data.encoded_len());
            // Wake the demux task in case it is blocked on the byte budget.
            mem_available_notify.notify_one();
            let _ = &task; // <- keep the task that polls data from the network alive.
            data
        });
        let stream = FlightRecordBatchStream::new_from_flight_data(stream);
        let stream = stream.map_err(map_flight_to_datafusion_error);
        let stream = stream.with_elapsed_compute(self.elapsed_compute.clone());

        // When the stream is dropped, cancel the background task to ensure prompt cleanup.
        let not_consumed_streams = Arc::clone(&self.not_consumed_streams);
        Ok(on_drop_stream(stream, move || {
            let remaining_streams = not_consumed_streams.fetch_sub(1, Ordering::SeqCst) - 1;
            if remaining_streams == 0 {
                cancel_token.cancel();
            }
        })
        .boxed())
    }
}

/// Equivalent to [RemoteWorkerConnection], but that pulls data from the local registry of tasks
/// rather than doing it across a gRPC interface.
#[cfg(feature = "flight")]
pub(crate) struct LocalWorkerConnection {
    partition_start: usize,
    local_streams: Vec<Mutex<Option<BoxStream<'static, Result<RecordBatch>>>>>,
}

#[cfg(feature = "flight")]
impl LocalWorkerConnection {
    pub(crate) fn init(
        input_stage: &RemoteStage,
        target_partition_range: Range<usize>,
        target_task: usize,
        producer_head: ProducerHead,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        MetricBuilder::new(metrics)
            .global_counter("local_connections_used")
            .add(1);
        let Some(lw_ctx) = ctx.session_config().get_extension::<LocalWorkerContext>() else {
            return exec_err!("Missing LocalWorkerContext extension");
        };

        let task_key = TaskKey {
            query_id: serialize_uuid(&input_stage.query_id),
            stage_id: input_stage.num as u64,
            task_number: target_task as u64,
        };

        let partition_start = target_partition_range.start;
        let mut local_streams = Vec::with_capacity(target_partition_range.len());
        for partition_i in target_partition_range {
            let request = ExecuteTaskRequest {
                task_key: Some(task_key.clone()),
                target_partition_start: partition_i as u64,
                target_partition_end: (partition_i + 1) as u64,
                producer_head: Some(producer_head.to_proto(ctx)?),
            };

            let task_data_entries = Arc::clone(&lw_ctx.task_data_entries);

            // The relevant entry from `task_data_entries` needs to be eagerly retrieved, it cannot be
            // left for until someone decides to start polling the returned `BoxStream`, otherwise,
            // there's risk that the entry is evicted by Moka's TTL, and by the time the returned stream
            // is polled, the entry might not be there.
            //
            // Note that this does not start polling the returned streams, it just instantiates them.
            let streams_future = SpawnedTask::spawn(async move {
                let (streams, _) = execute_local_task(&task_data_entries, request).await?;
                Ok::<_, DataFusionError>(streams)
            });

            let stream = async move {
                let mut streams = streams_future
                    .await
                    .map_err(|err| internal_datafusion_err!("{err}"))??;
                if streams.len() != 1 {
                    return internal_err!("Expected exactly 1 local stream");
                }
                Ok(streams.swap_remove(0))
            }
            .try_flatten_stream()
            .boxed();

            local_streams.push(Mutex::new(Some(stream)));
        }

        Ok(Self {
            partition_start,
            local_streams,
        })
    }
}

#[cfg(feature = "flight")]
impl WorkerConnection for LocalWorkerConnection {
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let Some(relative_i) = partition.checked_sub(self.partition_start) else {
            return internal_err!(
                "LocalWorkerConnection received an invalid partition {partition}, the starting partition is {}",
                self.partition_start
            );
        };
        let Some(slot) = self.local_streams.get(relative_i) else {
            return internal_err!(
                "LocalWorkerConnection has no stream for partition {partition}. Was it already consumed?"
            );
        };
        slot.lock().unwrap().take().ok_or_else(|| {
            internal_datafusion_err!(
                "LocalWorkerConnection stream for partition {partition} was already consumed"
            )
        })
    }
}

#[cfg(feature = "flight")]
fn fanout(o_txs: &[UnboundedSender<WorkerMsg>], err: Status) {
    for o_tx in o_txs {
        let _ = o_tx.send(Err(err.clone()));
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

#[cfg(feature = "flight")]
impl Debug for RemoteWorkerConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnection").finish()
    }
}

#[cfg(feature = "flight")]
trait ElapsedComputeFutureExt: Future + Sized {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeFuture<Self>;
}

#[cfg(feature = "flight")]
trait ElapsedComputeStreamExt: Stream + Sized {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeStream<Self>;
}

#[cfg(feature = "flight")]
impl<O, F: Future<Output = O>> ElapsedComputeFutureExt for F {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeFuture<Self> {
        ElapsedComputeFuture {
            inner: self,
            curr: Duration::default(),
            elapsed_compute,
        }
    }
}

#[cfg(feature = "flight")]
impl<O, S: Stream<Item = O>> ElapsedComputeStreamExt for S {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeStream<Self> {
        ElapsedComputeStream {
            inner: self,
            curr: Duration::default(),
            elapsed_compute,
        }
    }
}

#[cfg(feature = "flight")]
#[pin_project(PinnedDrop)]
struct ElapsedComputeStream<T> {
    #[pin]
    inner: T,
    curr: Duration,
    elapsed_compute: Time,
}

/// Drop implementation that ensures that any accumulated time is properly dumped to the metric
/// in case the stream gets dropped before completion.
#[cfg(feature = "flight")]
#[pinned_drop]
impl<T> PinnedDrop for ElapsedComputeStream<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.curr > Duration::default() {
            let self_projected = self.project();
            self_projected
                .elapsed_compute
                .add_duration(*self_projected.curr);
        }
    }
}

#[cfg(feature = "flight")]
impl<O, F: Stream<Item = O>> Stream for ElapsedComputeStream<F> {
    type Item = O;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let self_projected = self.project();
        let start = Instant::now();
        let result = self_projected.inner.poll_next(cx);
        *self_projected.curr += start.elapsed();
        if result.is_ready() {
            self_projected
                .elapsed_compute
                .add_duration(*self_projected.curr);
            *self_projected.curr = Duration::default();
        }
        result
    }
}

#[cfg(feature = "flight")]
#[pin_project(PinnedDrop)]
struct ElapsedComputeFuture<T> {
    #[pin]
    inner: T,
    curr: Duration,
    elapsed_compute: Time,
}

/// Drop implementation that ensures that any accumulated time is properly dumped to the metric
/// in case the future gets dropped before completion.
#[cfg(feature = "flight")]
#[pinned_drop]
impl<T> PinnedDrop for ElapsedComputeFuture<T> {
    fn drop(self: Pin<&mut Self>) {
        if self.curr > Duration::default() {
            let self_projected = self.project();
            self_projected
                .elapsed_compute
                .add_duration(*self_projected.curr);
        }
    }
}

#[cfg(feature = "flight")]
impl<O, F: Future<Output = O>> Future for ElapsedComputeFuture<F> {
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let self_projected = self.project();
        let start = Instant::now();
        let result = self_projected.inner.poll(cx);
        *self_projected.curr += start.elapsed();
        if result.is_ready() {
            self_projected
                .elapsed_compute
                .add_duration(*self_projected.curr);
            *self_projected.curr = Duration::default();
        }
        result
    }
}

#[cfg(all(test, feature = "flight"))]
mod tests {
    use super::*;
    use futures::StreamExt;
    use futures::stream::unfold;

    #[tokio::test]
    async fn elapsed_compute_future() {
        async fn cheap() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        async fn expensive() {
            let mut _count = 0f64;
            for i in 0..100000 {
                tokio::task::yield_now().await;
                _count /= i as f64
            }
        }

        let cheap_time = Time::new();
        cheap().with_elapsed_compute(cheap_time.clone()).await;
        println!("cheap future: {}", cheap_time.value());

        let expensive_time = Time::new();
        expensive()
            .with_elapsed_compute(expensive_time.clone())
            .await;
        println!("expensive future: {}", expensive_time.value());

        assert!(expensive_time.value() > cheap_time.value());
    }

    #[tokio::test]
    async fn elapsed_compute_stream() {
        fn cheap() -> impl Stream<Item = i64> {
            unfold(0i64, |state| async move {
                if state < 10 {
                    tokio::time::sleep(Duration::from_micros(10)).await;
                    Some((state, state + 1))
                } else {
                    None
                }
            })
        }

        fn expensive() -> impl Stream<Item = i64> {
            unfold(0i64, |state| async move {
                if state < 10 {
                    // Simulate expensive computation
                    let mut _count = 0f64;
                    for i in 1..100000 {
                        _count += (i as f64).sqrt();
                    }
                    tokio::task::yield_now().await;
                    Some((state, state + 1))
                } else {
                    None
                }
            })
        }

        let cheap_time = Time::new();
        cheap()
            .with_elapsed_compute(cheap_time.clone())
            .collect::<Vec<_>>()
            .await;
        println!("cheap future: {}", cheap_time.value());

        let expensive_time = Time::new();
        expensive()
            .with_elapsed_compute(expensive_time.clone())
            .collect::<Vec<_>>()
            .await;
        println!("expensive future: {}", expensive_time.value());

        assert!(expensive_time.value() > cheap_time.value());
    }
}
