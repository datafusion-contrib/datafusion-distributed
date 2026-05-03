use crate::common::{OnceLockResult, on_drop_stream, serialize_uuid};
use crate::metrics::LatencyMetricExt;
use crate::networking::get_distributed_channel_resolver;
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::{datafusion_error_to_tonic_status, map_flight_to_datafusion_error};
use crate::stage::RemoteStage;
use crate::worker::generated::worker::FlightAppMetadata;
use crate::worker::generated::worker::{ExecuteTaskRequest, TaskKey};
use crate::worker::impl_execute_task::execute_local_task;
use crate::worker::worker_service::TaskDataEntries;
use crate::{BytesMetricExt, ChannelResolver, DistributedConfig};
use arrow_flight::FlightData;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use dashmap::DashMap;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricValue};
use datafusion::physical_plan::metrics::{MetricBuilder, Time};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use http::Extensions;
use pin_project::{pin_project, pinned_drop};
use prost::Message;
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};
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

/// Holds a list of lazily initialized [WorkerConnection]s. Each position in the underlying
/// `connections` vector corresponds to the connection to one worker. It assumes a 1:1 mapping
/// between worker and tasks, and upon calling [WorkerConnectionPool::get_or_init_worker_connection]
/// it will initialize the corresponding position in the vector matching the provided `target_task`
/// index.
pub(crate) struct WorkerConnectionPool {
    connections: Vec<OnceLockResult<Box<dyn WorkerConnection + Sync + Send>>>,
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
        consumer_partitions: usize,
        consumer_task_count: usize,
        ctx: &Arc<TaskContext>,
    ) -> Result<&(dyn WorkerConnection + Sync + Send)> {
        let Some(worker_connection) = self.connections.get(target_task) else {
            return internal_err!(
                "WorkerConnections: Task index {target_task} not found, only have {} tasks",
                self.connections.len()
            );
        };

        let conn = worker_connection.get_or_init(|| {
            let Some(target_url) = input_stage.workers.get(target_task) else {
                internal_err!("input_stage.workers[{target_task}] out of range.")?
            };
            if let Some(lw_ctx) = ctx.session_config().get_extension::<LocalWorkerContext>()
                && &lw_ctx.self_url == target_url
            {
                // Instead of making a gRPC call to ourselves, better to just use local comms.
                Ok(Box::new(LocalWorkerConnection::init(
                    input_stage,
                    target_partitions,
                    target_task,
                    consumer_partitions,
                    consumer_task_count,
                    lw_ctx,
                    &self.metrics,
                )) as Box<_>)
            } else {
                // We are trying to reach a URL different from ours, so use normal gRPC streams.
                RemoteWorkerConnection::init(
                    input_stage,
                    target_partitions,
                    target_task,
                    consumer_partitions,
                    consumer_task_count,
                    ctx,
                    &self.metrics,
                )
                .map(|v| Box::new(v) as Box<_>)
                .map_err(Arc::new)
            }
        });

        match conn {
            Ok(v) => Ok(v.as_ref()),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}

type WorkerMsg = Result<(FlightData, FlightAppMetadata), Status>;

/// Abstraction that allows treating remote and local comms as equal. Network boundaries do not
/// care if the stream comes over the wire or locally.
pub(crate) trait WorkerConnection {
    /// Streams the specified partition. Consumers do not care if the implementation pulls data
    /// from in-memory or from local comms.
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>>;
}

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
struct RemoteWorkerConnection {
    task: Arc<SpawnedTask<()>>,
    not_consumed_streams: Arc<AtomicUsize>,
    cancel_token: CancellationToken,
    per_partition_rx: DashMap<usize, UnboundedReceiver<WorkerMsg>>,

    // Signals the demux task that buffered memory has been freed by a consumer.
    mem_available_notify: Arc<Notify>,

    // Metrics collection stuff.
    memory_reservation: Arc<MemoryReservation>,
    elapsed_compute: Time,
}

impl RemoteWorkerConnection {
    fn init(
        input_stage: &RemoteStage,
        target_partition_range: Range<usize>,
        target_task: usize,
        consumer_partitions: usize,
        consumer_task_count: usize,
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
                consumer_partitions: consumer_partitions as u64,
                consumer_task_count: consumer_task_count as u64,
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
                Err(err) => {
                    return fanout(&per_partition_tx, datafusion_error_to_tonic_status(&err));
                }
            };

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
                    return; // channel closed
                };
            }
        }.with_elapsed_compute(elapsed_compute));

        Ok(Self {
            task: Arc::new(task),
            cancel_token,
            not_consumed_streams: Arc::new(AtomicUsize::new(per_partition_rx.len())),
            per_partition_rx,
            mem_available_notify,

            // metrics stuff
            memory_reservation: memory_reservation_clone,
            elapsed_compute: elapsed_compute_clone,
        })
    }
}

impl WorkerConnection for RemoteWorkerConnection {
    /// Streams the provided `partition` from the remote worker.
    ///
    /// Note that this does not issue a network request, the actual network request happened before
    /// in the init step, and is in charge of handling not only this `partition`, but also all the
    /// partitions passed in `target_partition_range`. This method just streams all the record
    /// batches belonging to the provided `partition` from an in-memory queue, but what populates
    /// this queue is [WorkerConnection::init].
    ///
    /// When the returned stream is dropped (e.g., due to query cancellation), the background task
    /// pulling from the Flight stream will be cancelled promptly.
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let Some((_, partition_receiver)) = self.per_partition_rx.remove(&partition) else {
            return internal_err!(
                "WorkerConnection has no stream for target partition {partition}. Was it already consumed?"
            );
        };
        let task = Arc::clone(&self.task);
        let cancel_token = self.cancel_token.clone();

        let stream = UnboundedReceiverStream::new(partition_receiver);
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
pub(crate) struct LocalWorkerConnection {
    lw_ctx: Arc<LocalWorkerContext>,
    request_template: ExecuteTaskRequest,
}

impl LocalWorkerConnection {
    fn init(
        input_stage: &RemoteStage,
        target_partition_range: Range<usize>,
        target_task: usize,
        consumer_partitions: usize,
        consumer_task_count: usize,
        lw_ctx: Arc<LocalWorkerContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        MetricBuilder::new(metrics)
            .global_counter("local_connections_used")
            .add(1);
        Self {
            lw_ctx,
            request_template: ExecuteTaskRequest {
                task_key: Some(TaskKey {
                    query_id: serialize_uuid(&input_stage.query_id),
                    stage_id: input_stage.num as u64,
                    task_number: target_task as u64,
                }),
                target_partition_start: target_partition_range.start as u64,
                target_partition_end: target_partition_range.end as u64,
                consumer_partitions: consumer_partitions as u64,
                consumer_task_count: consumer_task_count as u64,
            },
        }
    }
}

impl WorkerConnection for LocalWorkerConnection {
    fn execute(&self, partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let mut request = self.request_template.clone();
        request.target_partition_start = partition as u64;
        request.target_partition_end = (partition + 1) as u64;
        let task_data_entries = Arc::clone(&self.lw_ctx.task_data_entries);
        Ok(async move {
            let (mut streams, _) = execute_local_task(&task_data_entries, request).await?;
            if streams.len() != 1 {
                return internal_err!("Expected exactly 1 local stream");
            }
            Ok(streams.swap_remove(0))
        }
        .try_flatten_stream()
        .boxed())
    }
}

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

impl Debug for RemoteWorkerConnection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConnection").finish()
    }
}

trait ElapsedComputeFutureExt: Future + Sized {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeFuture<Self>;
}

trait ElapsedComputeStreamExt: Stream + Sized {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeStream<Self>;
}

impl<O, F: Future<Output = O>> ElapsedComputeFutureExt for F {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeFuture<Self> {
        ElapsedComputeFuture {
            inner: self,
            curr: Duration::default(),
            elapsed_compute,
        }
    }
}

impl<O, S: Stream<Item = O>> ElapsedComputeStreamExt for S {
    fn with_elapsed_compute(self, elapsed_compute: Time) -> ElapsedComputeStream<Self> {
        ElapsedComputeStream {
            inner: self,
            curr: Duration::default(),
            elapsed_compute,
        }
    }
}

#[pin_project(PinnedDrop)]
struct ElapsedComputeStream<T> {
    #[pin]
    inner: T,
    curr: Duration,
    elapsed_compute: Time,
}

/// Drop implementation that ensures that any accumulated time is properly dumped to the metric
/// in case the stream gets dropped before completion.
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

#[pin_project(PinnedDrop)]
struct ElapsedComputeFuture<T> {
    #[pin]
    inner: T,
    curr: Duration,
    elapsed_compute: Time,
}

/// Drop implementation that ensures that any accumulated time is properly dumped to the metric
/// in case the future gets dropped before completion.
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

#[cfg(test)]
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
