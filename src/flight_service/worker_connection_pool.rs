use crate::Stage;
use crate::common::on_drop_stream;
use crate::config_extension_ext::get_config_extension_propagation_headers;
use crate::flight_service::do_get::DoGet;
use crate::metrics::latency_tracker::LatencyTracker;
use crate::networking::get_distributed_channel_resolver;
use crate::passthrough_headers::get_passthrough_headers;
use crate::protobuf::{
    FlightAppMetadata, StageKey, datafusion_error_to_tonic_status, map_flight_to_datafusion_error,
};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::{FlightData, Ticket};
use bytes::Bytes;
use dashmap::DashMap;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result, internal_err};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricValue};
use datafusion::physical_plan::metrics::{MetricBuilder, Time};
use futures::{Stream, TryStreamExt};
use http::Extensions;
use pin_project::{pin_project, pinned_drop};
use prost::Message;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

/// Holds a list of lazily initialized [WorkerConnection]s. Each position in the underlying
/// `connections` vector corresponds to the connection to one worker. It assumes a 1:1 mapping
/// between worker and tasks, and upon calling [WorkerConnectionPool::get_or_init_worker_connection]
/// it will initialize the corresponding position in the vector matching the provided `target_task`
/// index.
pub(crate) struct WorkerConnectionPool {
    connections: Vec<OnceLock<Result<WorkerConnection, Arc<DataFusionError>>>>,
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
        input_stage: &Stage,
        target_partitions: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
    ) -> Result<&WorkerConnection> {
        let Some(worker_connection) = self.connections.get(target_task) else {
            return internal_err!(
                "WorkerConnections: Task index {target_task} not found, only have {} tasks",
                self.connections.len()
            );
        };

        let conn = worker_connection.get_or_init(|| {
            WorkerConnection::init(
                input_stage,
                target_partitions,
                target_task,
                ctx,
                &self.metrics,
            )
            .map_err(Arc::new)
        });

        match conn {
            Ok(v) => Ok(v),
            Err(err) => Err(DataFusionError::Shared(Arc::clone(err))),
        }
    }
}

type WorkerMsg = Result<(FlightData, FlightAppMetadata, MemoryReservation), Status>;

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
pub(crate) struct WorkerConnection {
    task: Arc<SpawnedTask<()>>,
    not_consumed_streams: Arc<AtomicUsize>,
    cancel_token: CancellationToken,
    per_partition_rx: DashMap<usize, UnboundedReceiver<WorkerMsg>>,

    // Metrics collection stuff.
    curr_mem_used: Arc<AtomicUsize>,
    elapsed_compute: Time,
}

impl WorkerConnection {
    fn init(
        input_stage: &Stage,
        target_partition_range: Range<usize>,
        target_task: usize,
        ctx: &Arc<TaskContext>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Self> {
        let channel_resolver = get_distributed_channel_resolver(ctx.as_ref());

        // Stuff for collecting metrics.
        let curr_mem_used = Arc::new(AtomicUsize::new(0));
        let curr_mem_used_clone = Arc::clone(&curr_mem_used);

        // Track the maximum memory used to buffer recieved messages.
        let mut curr_max_mem = 0;
        let max_mem_used = MetricBuilder::new(metrics).global_gauge("max_mem_used");
        // Track the total encoded size of all recieved messages.
        let bytes_transferred = MetricBuilder::new(metrics).global_counter("bytes_transferred");
        // Track end-to-end network latency distribution for all messages.
        let network_latency_metrics = NetworkLatencyMetrics::new(metrics);
        // Track the total CPU time spent in polling messages over the network + decoding them.
        let elapsed_compute = Time::new();
        let elapsed_compute_clone = elapsed_compute.clone();
        MetricBuilder::new(metrics).build(MetricValue::ElapsedCompute(elapsed_compute.clone()));

        // Building the actual request that will be sent to the worker.
        let mut headers = get_config_extension_propagation_headers(ctx.session_config())?;
        headers.extend(get_passthrough_headers(ctx.session_config()));
        let ticket = Request::from_parts(
            MetadataMap::from_headers(headers),
            Extensions::default(),
            Ticket {
                ticket: DoGet {
                    plan_proto: Bytes::clone(input_stage.plan.encoded()?),
                    target_partition_start: target_partition_range.start as u64,
                    target_partition_end: target_partition_range.end as u64,
                    stage_key: Some(StageKey::new(
                        Bytes::from(input_stage.query_id.as_bytes().to_vec()),
                        input_stage.num as u64,
                        target_task as u64,
                    )),
                    target_task_index: target_task as u64,
                    target_task_count: input_stage.tasks.len() as u64,
                }
                .encode_to_vec()
                .into(),
            },
        );

        let Some(task) = input_stage.tasks.get(target_task) else {
            return internal_err!("ProgrammingError: Task {target_task} not found");
        };
        let Some(url) = task.url.clone() else {
            return internal_err!("ProgrammingError: task is unassigned, cannot proceed");
        };

        // The senders and receivers are unbounded queues used for multiplexing the record
        // batches sent through the single gRPC stream into one stream per partition.
        // The received record batches contain information of the partition to which they belong,
        // so we use that for determining where to put them.
        let mut per_partition_tx = Vec::with_capacity(target_partition_range.len());
        let per_partition_rx = DashMap::with_capacity(target_partition_range.len());
        for partition in target_partition_range.clone() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WorkerMsg>();
            per_partition_tx.push(tx);
            per_partition_rx.insert(partition, rx);
        }

        // We are retaining record batches in memory until they are consumed, so we need to account
        // for them in the memory pool.
        let memory_pool = Arc::clone(ctx.memory_pool());

        // Cancellation token allows us to stop the background task promptly when all partition
        // streams are dropped (e.g., when the query is cancelled).
        let cancel_token = CancellationToken::new();
        let cancel = cancel_token.clone();

        // This task will pull data from all the partitions in `target_partition_range`, and will
        // fan them out to the appropriate `per_partition_rx` based on the "partition" declared
        // in each individual record batch flight metadata.
        let task = SpawnedTask::spawn(async move {
            let mut client = match channel_resolver.get_flight_client_for_url(&url).await {
                Ok(v) => v,
                Err(err) => {
                    return fanout(&per_partition_tx, datafusion_error_to_tonic_status(&err));
                }
            };

            let mut interleaved_stream = match client.do_get(ticket).await {
                Ok(v) => v.into_inner(),
                Err(err) => return fanout(&per_partition_tx, err),
            };

            // Recorder updates network latency metrics when it's dropped.
            let mut network_latency_recorder =
                NetworkLatencyRecorder::new(network_latency_metrics);

            let consumer = MemoryConsumer::new("WorkerConnection");

            loop {
                // Check for cancellation while waiting for the next message.
                let msg = tokio::select! {
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

                let flight_metadata = match FlightAppMetadata::decode(msg.app_metadata.as_ref()) {
                    Ok(v) => v,
                    Err(err) => {
                        return fanout(&per_partition_tx, Status::internal(err.to_string()));
                    }
                };

                // Update the running latency tracker.
                network_latency_recorder.record_from_metadata(
                    &flight_metadata,
                    msg_received_time,
                );
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
                let mut reservation = consumer.clone_with_new_id().register(&memory_pool);
                let size = msg.encoded_len();

                // Update memory related metrics.
                bytes_transferred.add(size);
                let curr_mem_used = curr_mem_used.fetch_add(size, Ordering::Relaxed);
                if curr_mem_used > curr_max_mem {
                    curr_max_mem = curr_mem_used;
                    max_mem_used.set(curr_max_mem);
                }

                reservation.grow(size);
                if o_tx.send(Ok((msg, flight_metadata, reservation))).is_err() {
                    return; // channel closed
                };
            }
        }.with_elapsed_compute(elapsed_compute));

        Ok(Self {
            task: Arc::new(task),
            cancel_token,
            not_consumed_streams: Arc::new(AtomicUsize::new(per_partition_rx.len())),
            per_partition_rx,

            // metrics stuff
            curr_mem_used: curr_mem_used_clone,
            elapsed_compute: elapsed_compute_clone,
        })
    }

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
    pub(crate) fn stream_partition(
        &self,
        partition: usize,
        on_metadata: impl Fn(FlightAppMetadata) + Send + Sync + 'static,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + 'static> {
        let Some((_, partition_receiver)) = self.per_partition_rx.remove(&partition) else {
            return internal_err!(
                "WorkerConnection has no stream for target partition {partition}. Was it already consumed?"
            );
        };
        let task = Arc::clone(&self.task);
        let cancel_token = self.cancel_token.clone();
        let curr_mem_used = Arc::clone(&self.curr_mem_used);

        let stream = UnboundedReceiverStream::new(partition_receiver);
        let stream = stream.map_err(|err| FlightError::Tonic(Box::new(err)));
        let stream = stream.map_ok(move |(data, meta, reservation)| {
            curr_mem_used.fetch_sub(reservation.size(), Ordering::Relaxed);
            drop(reservation); // <- drop the reservation, freeing memory on the memory pool.
            let _ = &task; // <- keep the task that polls data from the network alive.
            on_metadata(meta);
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
        }))
    }
}

fn fanout(o_txs: &[UnboundedSender<WorkerMsg>], err: Status) {
    for o_tx in o_txs {
        let _ = o_tx.send(Err(err.clone()));
    }
}

/// Aggregates network latency metrics ourselves since DataFusion's default aggregation for
/// all [Time] metrics is sum.
#[derive(Clone)]
struct NetworkLatencyMetrics {
    first: Time,
    min: Time,
    max: Time,
    avg: Time,
    p99: Time,
}

impl NetworkLatencyMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            first: build_time_metric(metrics, "network_latency_first"),
            min: build_time_metric(metrics, "network_latency_min"),
            max: build_time_metric(metrics, "network_latency_max"),
            avg: build_time_metric(metrics, "network_latency_avg"),
            p99: build_time_metric(metrics, "network_latency_p99"),
        }
    }
}

fn build_time_metric(metrics: &ExecutionPlanMetricsSet, name: &'static str) -> Time {
    let time = Time::new();
    MetricBuilder::new(metrics).build(MetricValue::Time {
        name: name.into(),
        time: time.clone(),
    });
    time
}

/// Tracks message network latencies and publishes metrics on drop.
struct NetworkLatencyRecorder {
    tracker: LatencyTracker,
    metrics: NetworkLatencyMetrics,
}

impl NetworkLatencyRecorder {
    fn new(metrics: NetworkLatencyMetrics) -> Self {
        Self {
            tracker: LatencyTracker::new(),
            metrics,
        }
    }

    fn record_from_metadata(
        &mut self,
        flight_metadata: &FlightAppMetadata,
        message_recieved_time: SystemTime,
    ) {
        if flight_metadata.created_timestamp_unix_nanos == 0 {
            return;
        }

        let sent_time =
            UNIX_EPOCH + Duration::from_nanos(flight_metadata.created_timestamp_unix_nanos);
        if let Ok(delta) = message_recieved_time.duration_since(sent_time) {
            self.tracker.track(delta);
        }
    }
}

impl Drop for NetworkLatencyRecorder {
    fn drop(&mut self) {
        if let Some(first) = self.tracker.first() {
            self.metrics.first.add_duration(first);
        }
        if let Some(min) = self.tracker.min() {
            self.metrics.min.add_duration(min);
        }
        if let Some(max) = self.tracker.max() {
            self.metrics.max.add_duration(max);
        }
        if let Some(avg) = self.tracker.avg() {
            self.metrics.avg.add_duration(avg);
        }
        if let Some(p99) = self.tracker.quantile(0.99) {
            self.metrics.p99.add_duration(p99);
        }
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

impl Debug for WorkerConnection {
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

    #[test]
    fn network_latency_updates_on_drop() {
        let metrics = NetworkLatencyMetrics::new(&ExecutionPlanMetricsSet::default());

        let sent = UNIX_EPOCH + Duration::from_secs(5);
        let received = UNIX_EPOCH + Duration::from_secs(8);
        let mut flight_metadata = FlightAppMetadata::new(0);
        flight_metadata.created_timestamp_unix_nanos =
            sent.duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;

        {
            let mut recorder = NetworkLatencyRecorder::new(metrics.clone());
            recorder.record_from_metadata(&flight_metadata, received);
        }

        let expected = 3_000_000_000usize;
        assert_eq!(metrics.first.value(), expected);
        assert_eq!(metrics.min.value(), expected);
        assert_eq!(metrics.max.value(), expected);
        assert_eq!(metrics.avg.value(), expected);
        assert!(metrics.p99.value() >= expected);
    }
}
