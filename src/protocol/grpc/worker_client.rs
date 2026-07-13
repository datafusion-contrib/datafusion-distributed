use super::channel_resolver::BoxCloneSyncChannel;
use super::errors::{map_flight_to_datafusion_error, map_status_to_datafusion_error};
use super::generated::worker as pb;
use super::metrics_proto::metrics_set_proto_to_df;
use crate::common::serialize_uuid;
use crate::grpc::generated::worker::FlightAppMetadata;
use crate::grpc::on_drop_stream::on_drop_stream;
use crate::{
    BytesMetricExt, CoordinatorToWorkerMsg, DistributedConfig, ExecuteTaskRequest,
    FirstLatencyMetric, GetWorkerInfoRequest, GetWorkerInfoResponse, LatencyMetricExt, LoadInfo,
    MaxLatencyMetric, MinLatencyMetric, P50LatencyMetric, P95LatencyMetric, ProducerHeadSpec,
    SetPlanRequest, TaskKey, TaskMetrics, WorkUnitBatch, WorkUnitFeedDeclaration, WorkUnitMsg,
    WorkerChannel, WorkerToCoordinatorMsg,
};
use arrow_flight::FlightData;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_expr_common::metrics::{Count, MetricBuilder, MetricValue, Time};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use http::{Extensions, HeaderMap};
use pin_project::{pin_project, pinned_drop};
use prost::Message;
use std::borrow::Cow;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataMap;
use tonic::{Request, Status};

#[async_trait]
impl WorkerChannel for pb::worker_service_client::WorkerServiceClient<BoxCloneSyncChannel> {
    async fn coordinator_channel(
        &mut self,
        headers: HeaderMap,
        c2w_stream: BoxStream<'static, CoordinatorToWorkerMsg>,
    ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>> {
        let input_stream = c2w_stream.map(encode_coordinator_to_worker_msg);

        let output_stream = self
            .coordinator_channel(Request::from_parts(
                MetadataMap::from_headers(headers),
                Extensions::default(),
                input_stream,
            ))
            .boxed()
            .await
            .map_err(map_status_to_datafusion_error)?
            .into_inner()
            .map_err(map_status_to_datafusion_error)
            .map(|msg| decode_worker_to_coordinator_msg(msg?))
            .boxed();

        Ok(output_stream)
    }

    async fn execute_task(
        &mut self,
        headers: HeaderMap,
        request: ExecuteTaskRequest,
        metrics: ExecutionPlanMetricsSet,
        ctx: &Arc<TaskContext>,
    ) -> Result<Vec<BoxStream<'static, Result<RecordBatch>>>> {
        let d_cfg = DistributedConfig::from_session_config(ctx.session_config())?;
        let buffer_budget_bytes = d_cfg.worker_connection_buffer_budget_bytes;

        // We are retaining record batches in memory until they are consumed, so we need to account
        // for them in the memory pool.
        let memory_reservation =
            Arc::new(MemoryConsumer::new("WorkerConnection").register(ctx.memory_pool()));
        let memory_reservation_clone = Arc::clone(&memory_reservation);

        // Track the maximum memory used to buffer recieved messages.
        let mut curr_max_mem = 0;
        let max_mem_used = MetricBuilder::new(&metrics).global_gauge("max_mem_used");
        // Track the total encoded size of all recieved messages.
        let bytes_transferred = MetricBuilder::new(&metrics).bytes_counter("bytes_transferred");
        let msg_count = MetricBuilder::new(&metrics).global_counter("msg_count");
        // Track end-to-end network latency distribution for messages that actually arrive.
        let mut latency_metrics = NetworkLatencyMetrics::new(&metrics);
        // Track the total CPU time spent in polling messages over the network + decoding them.
        let elapsed_compute = Time::new();
        let elapsed_compute_clone = elapsed_compute.clone();
        MetricBuilder::new(&metrics).build(MetricValue::ElapsedCompute(elapsed_compute.clone()));

        let target_partition_range = request.target_partition_start..request.target_partition_end;
        let request = pb::ExecuteTaskRequest {
            task_key: Some(encode_task_key(request.task_key)),
            target_partition_start: request.target_partition_start as u64,
            target_partition_end: request.target_partition_end as u64,
            producer_head: Some(encode_producer_head_spec(request.producer_head_spec)),
        };
        let metadata = MetadataMap::from_headers(headers);

        // The senders and receivers are unbounded queues used for multiplexing the record
        // batches sent through the single gRPC stream into one stream per partition. They
        // are unbounded to avoid head-of-line blocking: a single bounded queue could block
        // the demux task and starve all sibling partitions even though they have capacity,
        // which deadlocks queries with cross-partition dependencies.
        // Total memory is bounded globally below via `mem_available_notify`.
        let mut per_partition_tx = Vec::with_capacity(target_partition_range.len());
        let mut per_partition_rx = Vec::with_capacity(target_partition_range.len());
        for _partition in target_partition_range.clone() {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<WorkerMsg>();
            per_partition_tx.push(tx);
            per_partition_rx.push(rx);
        }

        let mem_available_notify = Arc::new(Notify::new());
        let mem_available_notify_for_task = Arc::clone(&mem_available_notify);

        let first_poll_notify = Arc::new(Notify::new());
        let first_poll_notify_for_task = Arc::clone(&first_poll_notify);

        // Cancellation token allows us to stop the background task promptly when all partition
        // streams are dropped (e.g., when the query is cancelled).
        let cancel_token = CancellationToken::new();
        let cancel = cancel_token.clone();

        let mut self_clone = self.clone();
        let request_for_task = request.clone();
        let metadata_for_task = metadata.clone();

        // This task will pull data from all the partitions in `target_partition_range`, and will
        // fan them out to the appropriate `per_partition_rx` based on the "partition" declared
        // in each individual record batch flight metadata.
        let task = SpawnedTask::spawn(async move {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    // If all SendableRecordBatchStreams canceled before any poll, we need to
                    // anyway trigger the task execution and cancel it immediately so that the
                    // cancellation is propagated also in the remote worker. Otherwise, it might
                    // hang forever waiting for someone to execute it.
                    let _ = self_clone.execute_task(Request::from_parts(
                        metadata_for_task,
                        Extensions::default(),
                        request_for_task,
                    )).await;
                    return
                },
                _ = first_poll_notify_for_task.notified() => {}
            }

            let request = Request::from_parts(
                metadata_for_task,
                Extensions::default(),
                request_for_task,
            );
            let mut interleaved_stream = match self_clone.execute_task(request).await {
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
                    latency_metrics.add_duration(delta);
                }

                let partition = flight_metadata.partition as usize;
                // the `per_partition_tx` variable is using a normal `Vec` for storing the
                // channel transmitters, so we need to subtract the `target_partition_range.start`
                // to the `partition` in order to offset it to the appropriate index.
                let Some(sender_i) = partition.checked_sub(target_partition_range.start) else {
                    let msg = format!(
                        "Received partition {partition} in Flight metadata, but available partitions are {target_partition_range:?}"
                    );
                    return fanout(&per_partition_tx, Status::internal(msg));
                };

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

        let task = Arc::new(task);
        let not_consumed_streams = Arc::new(AtomicUsize::new(per_partition_rx.len()));

        let mut result = Vec::with_capacity(per_partition_rx.len());
        for partition_receiver in per_partition_rx {
            let task = Arc::clone(&task);
            let cancel_token = cancel_token.clone();

            let first_poll_notify = Arc::clone(&first_poll_notify);
            let stream = async move {
                first_poll_notify.notify_one();
                UnboundedReceiverStream::new(partition_receiver)
            }
            .flatten_stream();

            let stream = stream.map_err(|err| FlightError::Tonic(Box::new(err)));
            let reservation = Arc::clone(&memory_reservation_clone);
            let mem_available_notify = Arc::clone(&mem_available_notify);
            let stream = stream.map_ok(move |(data, _meta)| {
                reservation.shrink(data.encoded_len());
                // Wake the demux task in case it is blocked on the byte budget.
                mem_available_notify.notify_one();
                let _ = &task; // <- keep the task that polls data from the network alive.
                data
            });
            let stream = FlightRecordBatchStream::new_from_flight_data(stream);
            let stream = stream.map_err(map_flight_to_datafusion_error);
            let stream = stream.with_elapsed_compute(elapsed_compute_clone.clone());

            // When the stream is dropped, cancel the background task to ensure prompt cleanup.
            let not_consumed_streams = Arc::clone(&not_consumed_streams);
            result.push(
                on_drop_stream(stream, move || {
                    let remaining_streams = not_consumed_streams.fetch_sub(1, Ordering::SeqCst) - 1;
                    if remaining_streams == 0 {
                        cancel_token.cancel();
                    }
                })
                .boxed(),
            );
        }

        Ok(result)
    }

    async fn get_worker_info(
        &mut self,
        _request: GetWorkerInfoRequest,
    ) -> Result<GetWorkerInfoResponse> {
        let response = self
            .get_worker_info(pb::GetWorkerInfoRequest {})
            .await
            .map_err(map_status_to_datafusion_error)?
            .into_inner();
        Ok(GetWorkerInfoResponse {
            version: response.version,
        })
    }
}

type WorkerMsg = Result<(FlightData, FlightAppMetadata), Status>;

struct NetworkLatencyMetrics {
    metrics: ExecutionPlanMetricsSet,
    values: Option<NetworkLatencyMetricValues>,
}

impl NetworkLatencyMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            metrics: metrics.clone(),
            values: None,
        }
    }

    fn add_duration(&mut self, duration: Duration) {
        self.values
            .get_or_insert_with(|| NetworkLatencyMetricValues::new(&self.metrics))
            .add_duration(duration);
    }
}

struct NetworkLatencyMetricValues {
    min_latency: MinLatencyMetric,
    max_latency: MaxLatencyMetric,
    p50_latency: P50LatencyMetric,
    p95_latency: P95LatencyMetric,
    first_latency: FirstLatencyMetric,
    sum_latency: Time,
    latency_count: Count,
}

impl NetworkLatencyMetricValues {
    fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
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

        Self {
            min_latency,
            max_latency,
            p50_latency,
            p95_latency,
            first_latency,
            sum_latency,
            latency_count,
        }
    }

    fn add_duration(&self, duration: Duration) {
        self.min_latency.add_duration(duration);
        self.max_latency.add_duration(duration);
        self.p50_latency.add_duration(duration);
        self.p95_latency.add_duration(duration);
        self.first_latency.add_duration(duration);
        self.sum_latency.add_duration(duration);
        self.latency_count.add(1);
    }
}

pub(super) fn encode_producer_head_spec(
    head: ProducerHeadSpec,
) -> pb::execute_task_request::ProducerHead {
    match head {
        ProducerHeadSpec::None => pb::execute_task_request::ProducerHead::None(pb::NoneHead {}),
        ProducerHeadSpec::BroadcastExec { output_partitions } => {
            pb::execute_task_request::ProducerHead::Broadcast(pb::BroadcastExecHead {
                output_partitions: output_partitions as u64,
            })
        }
        ProducerHeadSpec::RepartitionExec { partitioning } => {
            pb::execute_task_request::ProducerHead::Repartition(pb::RepartitionExecHead {
                partitioning,
            })
        }
    }
}

fn encode_coordinator_to_worker_msg(msg: CoordinatorToWorkerMsg) -> pb::CoordinatorToWorkerMsg {
    pb::CoordinatorToWorkerMsg {
        inner: Some(match msg {
            CoordinatorToWorkerMsg::SetPlanRequest(request) => {
                pb::coordinator_to_worker_msg::Inner::SetPlanRequest(encode_set_plan_request(
                    request,
                ))
            }
            CoordinatorToWorkerMsg::WorkUnitBatch(batch) => {
                pb::coordinator_to_worker_msg::Inner::WorkUnitBatch(encode_work_unit_batch(batch))
            }
            CoordinatorToWorkerMsg::WorkUnitEos => {
                pb::coordinator_to_worker_msg::Inner::WorkUnitEos(true)
            }
        }),
    }
}

fn encode_set_plan_request(request: SetPlanRequest) -> pb::SetPlanRequest {
    pb::SetPlanRequest {
        task_key: Some(encode_task_key(request.task_key)),
        task_count: request.task_count as u64,
        plan_proto: request.plan_proto,
        work_unit_feed_declarations: request
            .work_unit_feed_declarations
            .into_iter()
            .map(encode_work_unit_feed_declaration)
            .collect(),
        target_worker_url: request.target_worker_url.to_string(),
        query_start_time_ns: request.query_start_time_ns as u64,
    }
}

fn encode_work_unit_batch(batch: WorkUnitBatch) -> pb::WorkUnitBatch {
    pb::WorkUnitBatch {
        batch: batch.batch.into_iter().map(encode_work_unit).collect(),
    }
}

fn encode_work_unit(work_unit: WorkUnitMsg) -> pb::WorkUnit {
    pb::WorkUnit {
        id: serialize_uuid(&work_unit.id),
        partition: work_unit.partition as u64,
        body: work_unit.body,
        created_timestamp_unix_nanos: work_unit.created_timestamp_unix_nanos as u64,
        sent_timestamp_unix_nanos: work_unit.sent_timestamp_unix_nanos as u64,
        received_timestamp_unix_nanos: work_unit.received_timestamp_unix_nanos as u64,
        processed_timestamp_unix_nanos: work_unit.processed_timestamp_unix_nanos as u64,
    }
}

fn encode_work_unit_feed_declaration(
    declaration: WorkUnitFeedDeclaration,
) -> pb::set_plan_request::WorkUnitFeedDeclaration {
    pb::set_plan_request::WorkUnitFeedDeclaration {
        id: serialize_uuid(&declaration.id),
        partitions: declaration.partitions as u64,
    }
}

fn encode_task_key(task_key: TaskKey) -> pb::TaskKey {
    pb::TaskKey {
        query_id: serialize_uuid(&task_key.query_id),
        stage_id: task_key.stage_id as u64,
        task_number: task_key.task_number as u64,
    }
}

fn decode_worker_to_coordinator_msg(
    msg: pb::WorkerToCoordinatorMsg,
) -> Result<WorkerToCoordinatorMsg> {
    Ok(
        match msg
            .inner
            .ok_or_else(|| missing("WorkerToCoordinatorMsg.inner"))?
        {
            pb::worker_to_coordinator_msg::Inner::TaskMetrics(task_metrics) => {
                WorkerToCoordinatorMsg::TaskMetrics(decode_task_metrics(task_metrics)?)
            }
            pb::worker_to_coordinator_msg::Inner::LoadInfo(load_info) => {
                WorkerToCoordinatorMsg::LoadInfo(decode_load_info(load_info))
            }
            pb::worker_to_coordinator_msg::Inner::LoadInfoEos(_) => {
                WorkerToCoordinatorMsg::LoadInfoEos
            }
        },
    )
}

fn decode_task_metrics(task_metrics: pb::TaskMetrics) -> Result<TaskMetrics> {
    Ok(TaskMetrics {
        pre_order_plan_metrics: task_metrics
            .pre_order_plan_metrics
            .into_iter()
            .map(|metrics_set| metrics_set_proto_to_df(&metrics_set))
            .collect::<Result<_>>()?,
        task_metrics: metrics_set_proto_to_df(
            &task_metrics
                .task_metrics
                .ok_or_else(|| missing("task_metrics"))?,
        )?,
    })
}

fn decode_load_info(load_info: pb::LoadInfo) -> LoadInfo {
    LoadInfo {
        partition: load_info.partition as usize,
        rows_ready: load_info.rows_ready as usize,
        per_column_bytes_ready: load_info
            .per_column_bytes_ready
            .into_iter()
            .map(|bytes| bytes as usize)
            .collect(),
        per_column_ndv_percentage: load_info.per_column_ndv_percentage,
        per_column_null_percentage: load_info.per_column_null_percentage,
        rows_pulled_from_leaf: load_info.rows_pulled_from_leaf as usize,
        reached_eos: load_info.reached_eos,
    }
}

fn missing(field: &'static str) -> DataFusionError {
    DataFusionError::Internal(format!("Missing field '{field}'"))
}

fn fanout(o_txs: &[UnboundedSender<WorkerMsg>], err: Status) {
    for o_tx in o_txs {
        let _ = o_tx.send(Err(err.clone()));
    }
}

/// Creates a [`WorkerServiceClient`] with high default message size limits.
///
/// This is a convenience function that wraps [`WorkerServiceClient::new`] and configures
/// it with `max_decoding_message_size(usize::MAX)` and `max_encoding_message_size(usize::MAX)`
/// to avoid message size limitations for internal communication.
///
/// Users implementing custom [`ChannelResolver`]s should use this function in their
/// `get_worker_client_for_url` implementations to ensure consistent behavior with built-in
/// implementations.
///
/// # Example
///
/// ```rust
/// # use datafusion::common::DataFusionError;
/// # use datafusion::error::Result;
/// # use tonic::transport::Channel;
/// # use url::Url;
/// # use datafusion_distributed::{ChannelResolver, WorkerChannel, grpc};
///
/// struct MyResolver;
///
/// #[async_trait::async_trait]
/// impl ChannelResolver for MyResolver {
///     async fn get_worker_client_for_url(&self, url: &Url) -> Result<Box<dyn WorkerChannel>> {
///         let channel = Channel::from_shared(url.to_string())
///             .map_err(|err| DataFusionError::External(Box::new(err)))?
///             .connect()
///             .await
///             .map_err(|err| DataFusionError::External(Box::new(err)))?;
///         Ok(grpc::create_worker_client(grpc::BoxCloneSyncChannel::new(channel)))
///     }
/// }
/// ```
pub fn create_worker_client(channel: BoxCloneSyncChannel) -> Box<dyn WorkerChannel> {
    Box::new(
        pb::worker_service_client::WorkerServiceClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX),
    )
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
