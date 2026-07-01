use crate::common::{require_one_child, vec_cast};
use crate::{
    BytesCounterMetric, BytesMetricExt, GaugeMetricExt, LatencyMetricExt, LoadInfo, MaxGaugeMetric,
    MaxLatencyMetric, P50LatencyMetric,
};
use datafusion::arrow::array::Array;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, exec_err};
use datafusion::common::{HashSet, ScalarValue};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::{Gauge, MetricValue, MetricsSet};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::oneshot;

/// How many [RecordBatch]s to allow the input stream to yield synchronously (without yielding back
/// to tokio) before short-circuiting buffering.
const READY_CHUNK_LIMIT: usize = 256;
/// Maximum read of bytes per second allowed to be emitted. Reads greater than this will be
/// truncated to this max value, as it's assumed that [READY_CHUNK_LIMIT] was hit and no useful
/// measurement can actually be emitted.
const MAX_BYTES_PER_SECOND: usize = 512 * 1024 * 1024;
/// Maximum number of rows per second allowed to be emitted. Reads greater than this will be
/// truncated to this max value, as it's assumed that [READY_CHUNK_LIMIT] was hit and no useful
/// measurement can actually be emitted.
const MAX_ROWS_PER_SECOND: usize = 1024 * 1024;
/// Maximum number of rows sampled from the peek queue when estimating per-column NDV.
const NDV_MAX_ROWS_SAMPLE: usize = 1000;

#[derive(Debug)]
pub struct SamplerExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) metric_set: ExecutionPlanMetricsSet,
    pub(crate) partition_samplers: Vec<PartitionSampler>,
    pub(crate) execution_started: Arc<AtomicBool>,
}

/// Metrics that quantify how long the sampler held data in memory before the consumer
/// (real execution) attached, plus the peak accumulated size reached. All metrics are shared
/// across the partition samplers; the latency metrics aggregate per-partition observations.
#[derive(Debug, Clone)]
pub(crate) struct SamplerExecMetrics {
    /// Time since [SamplerExec::kick_off_first_sampler] was called until the first batch from
    /// the input arrived
    kick_off_to_fist_batch_p50: P50LatencyMetric,
    kick_off_to_fist_batch_max: MaxLatencyMetric,
    /// Time since [SamplerExec::kick_off_first_sampler] was called until the [LoadInfo] message
    /// was sent.
    kick_off_to_load_info_sent_p50: P50LatencyMetric,
    kick_off_to_load_info_sent_max: MaxLatencyMetric,
    /// Time since [SamplerExec::kick_off_first_sampler] was called until the node was properly
    /// executed with [SamplerExec::execute].
    kick_off_to_execution_p50: P50LatencyMetric,
    kick_off_to_execution_max: MaxLatencyMetric,
    /// Maximum number of record batches peeked by a sampler.
    max_batches_peeked: MaxGaugeMetric,
    /// Peak memory accumulated by any partition sampler during the sampling phase.
    max_mem_used: Gauge,
    /// Bytes per second flowing through the sampler node.
    bytes_per_sec: BytesCounterMetric,
    /// Bytes ready at the moment of reporting load info.
    bytes_ready: BytesCounterMetric,
    /// Elapsed compute while sampling.
    elapsed_compute: Time,
}

impl SamplerExecMetrics {
    fn new(metric_set: &ExecutionPlanMetricsSet) -> Self {
        let bdr = || MetricBuilder::new(metric_set);
        Self {
            kick_off_to_fist_batch_p50: bdr().p50_latency("kick_off_to_first_batch_p50"),
            kick_off_to_fist_batch_max: bdr().max_latency("kick_off_to_first_batch_max"),
            kick_off_to_load_info_sent_p50: bdr().p50_latency("kick_off_to_load_info_sent_p50"),
            kick_off_to_load_info_sent_max: bdr().max_latency("kick_off_to_load_info_sent_max"),
            kick_off_to_execution_p50: bdr().p50_latency("kick_off_to_execution_p50"),
            kick_off_to_execution_max: bdr().max_latency("kick_off_to_execution_max"),
            max_batches_peeked: bdr().max_gauge("max_batches_peeked"),
            max_mem_used: bdr().global_gauge("max_mem_used"),
            bytes_per_sec: bdr().bytes_counter("bytes_per_sec"),
            bytes_ready: bdr().bytes_counter("bytes_ready"),
            elapsed_compute: {
                let time = Time::new();
                bdr().build(MetricValue::ElapsedCompute(time.clone()));
                time
            },
        }
    }
}

impl SamplerExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let metric_set = ExecutionPlanMetricsSet::new();
        let metric_set_clone = metric_set.clone();
        // Metrics need to be lazily initialized, otherwise the coordinator side will register
        // them when they are never relevant there, they are just relevant in workers.
        //
        // If we don't do this, the [SamplerExec] constructed during planning will register its
        // own zeroed SamplerExecMetrics in the ExecutionPlanMetricsSet, even if the metrics we care
        // about are just the ones collected in workers during execution.
        let metrics: Arc<LazyLock<_, Box<dyn FnOnce() -> SamplerExecMetrics + Send>>> =
            Arc::new(LazyLock::new(Box::new(move || {
                SamplerExecMetrics::new(&metric_set_clone)
            })));
        let partitions = input.properties().partitioning.partition_count();
        let execution_started = Arc::new(AtomicBool::new(false));
        let mut samplers = Vec::with_capacity(partitions);
        for i in 0..partitions {
            samplers.push(PartitionSampler {
                partition_idx: i,
                input: Arc::clone(&input),
                stream: Mutex::new(None),
                metrics: Arc::clone(&metrics),
                kick_off_at: Arc::new(OnceLock::new()),
                first_batch_at: Arc::new(OnceLock::new()),
                load_info_sent_at: Arc::new(OnceLock::new()),
                execution_started: Arc::clone(&execution_started),
            });
        }
        Self {
            input,
            metric_set,
            partition_samplers: samplers,
            execution_started,
        }
    }

    pub(crate) fn kick_off_first_sampler(
        plan: Arc<dyn ExecutionPlan>,
        ctx: Arc<TaskContext>,
    ) -> Result<Vec<oneshot::Receiver<LoadInfo>>> {
        let mut receivers = vec![];
        plan.apply(|plan| {
            let Some(sampler) = plan.downcast_ref::<SamplerExec>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            receivers.reserve(sampler.partition_samplers.len());
            for partition_sampler in &sampler.partition_samplers {
                let rx = partition_sampler.kick_off(Arc::clone(&ctx))?;
                receivers.push(rx);
            }
            Ok(TreeNodeRecursion::Stop)
        })?;
        Ok(receivers)
    }
}

pub(crate) struct PartitionSampler {
    partition_idx: usize,
    input: Arc<dyn ExecutionPlan>,
    stream: Mutex<Option<SendableRecordBatchStream>>,
    execution_started: Arc<AtomicBool>,

    // Metrics state.
    metrics: Arc<LazyLock<SamplerExecMetrics, Box<dyn FnOnce() -> SamplerExecMetrics + Send>>>,
    /// Set when `kick_off` is invoked. Used at `execute()` time to record how long the
    /// sampler sampled data before the consumer attached.
    kick_off_at: Arc<OnceLock<Instant>>,
    /// Set the first time the producer task emits a `LoadInfo`. Used at `execute()` time
    /// to record the gap between the first sample and the consumer starting.
    first_batch_at: Arc<OnceLock<Instant>>,
    /// Set immediately after `sampling_tx.send()` succeeds. Used to measure the full
    /// round-trip: LoadInfo sent → coordinator collects votes → downstream plan dispatched
    /// → consumer calls execute().
    load_info_sent_at: Arc<OnceLock<Instant>>,
}

impl Debug for PartitionSampler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionSampler").finish()
    }
}

impl PartitionSampler {
    fn start_stream(&self) -> Option<SendableRecordBatchStream> {
        let Some(kick_off_at) = self.kick_off_at.get() else {
            return self.stream.lock().unwrap().take();
        };

        // Time since this sampler was kicked off until the first batch arrived.
        if let Some(t) = self.first_batch_at.get() {
            let delay = t.saturating_duration_since(*kick_off_at);
            self.metrics.kick_off_to_fist_batch_p50.add_duration(delay);
            self.metrics.kick_off_to_fist_batch_max.add_duration(delay);
        }

        // Time since the sampler was kicked off until the LoadInfo message was sent.
        if let Some(t) = self.load_info_sent_at.get() {
            let delay = t.saturating_duration_since(*kick_off_at);
            self.metrics
                .kick_off_to_load_info_sent_p50
                .add_duration(delay);
            self.metrics
                .kick_off_to_load_info_sent_max
                .add_duration(delay);
        }

        // Time since the sampler was kicked off until it started executing.
        let delay = kick_off_at.elapsed();
        self.metrics.kick_off_to_execution_p50.add_duration(delay);
        self.metrics.kick_off_to_execution_max.add_duration(delay);

        self.stream.lock().unwrap().take()
    }

    fn kick_off(&self, ctx: Arc<TaskContext>) -> Result<oneshot::Receiver<LoadInfo>> {
        let _ = self.kick_off_at.set(Instant::now());
        let (sampling_tx, sampling_rx) = oneshot::channel();

        let input = Arc::clone(&self.input);
        let partition_idx = self.partition_idx;
        let schema = input.schema();
        let elapsed_compute = self.metrics.elapsed_compute.clone();
        let first_batch_at = Arc::clone(&self.first_batch_at);
        let n_cols = self.input.schema().fields.len();

        let reporter = LoadInfoDropHandler {
            load_info: zero_load_info(partition_idx, n_cols),
            sampling_tx: Some(sampling_tx),
            bytes_per_second_metric: self.metrics.bytes_per_sec.clone(),
            load_info_sent_at: Arc::clone(&self.load_info_sent_at),
            bytes_ready_metric: self.metrics.bytes_ready.clone(),
            omit: Arc::clone(&self.execution_started),
        };

        let mut peek = RecordBatchPeek {
            peek: VecDeque::new(),
            n_cols,
            max_mem_used: self.metrics.max_mem_used.clone(),
            max_batches_peeked: self.metrics.max_batches_peeked.clone(),
            memory_reservation: Arc::new(
                MemoryConsumer::new(format!("PartitionSampler[{partition_idx}]"))
                    .register(ctx.memory_pool()),
            ),
            first_batch_at: Arc::clone(&self.first_batch_at),
        };

        // Execute the input synchronously so any setup error surfaces before we
        // spawn the producer task.
        let mut input_stream = input.execute(partition_idx, ctx)?.fuse();

        let task = SpawnedTask::spawn(async move {
            // First, read at once all the RecordBatches that are ready to be yielded synchronously.
            // Some downstream nodes will accumulate data in-memory, and will then yield several
            // RecordBatches at once synchronously (without Poll::Pending gaps in between).
            let mut chunked = (&mut input_stream).ready_chunks(READY_CHUNK_LIMIT);
            let Some(batches) = chunked.next().await else {
                // Not a single RecordBatch was produced, so let bytes_per_second=0 be sent as-is.
                return Ok(peek.chain(input_stream).boxed());
            };
            let _elapsed_compute_timer = elapsed_compute.timer();
            for batch in batches {
                let _ = first_batch_at.set(Instant::now());
                peek.push(batch?);
            }

            // Peek whether there is more data to be produced.
            if let Some(result) = input_stream.next().now_or_never() {
                return if let Some(batch) = result {
                    // A batch was immediately available without hitting an async gap (the input is
                    // still yielding synchronously). store it so its rows are not lost. We cannot
                    // measure a meaningful arrival velocity in this case, so as before, assume the
                    // worst.
                    peek.push(batch?);
                    reporter.report(&peek, MAX_BYTES_PER_SECOND, MAX_ROWS_PER_SECOND);
                    Ok(peek.chain(input_stream).boxed())
                } else {
                    // No more batches to read, so no velocity measurement.
                    reporter.report(&peek, 0, 0);
                    Ok(peek.chain(input_stream).boxed())
                };
            }

            drop(_elapsed_compute_timer);

            // Wait for an async gap in order to measure data velocity.
            let poll_start = Instant::now();
            let Some(batch) = input_stream.try_next().await? else {
                let _elapsed_compute_timer = elapsed_compute.timer();
                // The last message was somehow the last message in the stream, but the stream did
                // not end immediately. This is an unlikely scenario.
                reporter.report(&peek, 0, 0);
                return Ok(peek.chain(input_stream).boxed());
            };
            let _elapsed_compute_timer = elapsed_compute.timer();

            let bytes_per_second =
                (record_batch_size(&batch) as f32 / poll_start.elapsed().as_secs_f32()) as usize;
            let rows_per_second =
                (batch.num_rows() as f32 / poll_start.elapsed().as_secs_f32()) as usize;

            peek.push(batch);

            // Some RecordBatches where buffered, but there's more to be yielded, so both
            // bytes_per_second and bytes_ready can be reported.
            reporter.report(&peek, bytes_per_second, rows_per_second);

            Ok(peek.chain(input_stream).boxed())
        });

        let stream = async move {
            task.await
                .map_err(|err| DataFusionError::Internal(err.to_string()))?
        }
        .try_flatten_stream();

        self.stream
            .lock()
            .expect("poisoned lock")
            .replace(Box::pin(RecordBatchStreamAdapter::new(schema, stream)));

        Ok(sampling_rx)
    }
}

/// Wraps a [LoadInfo] and emits it on [Drop] through the provided [oneshot] channel.
///
/// Emitting on drop ensures that it's always emitted.
struct LoadInfoDropHandler {
    omit: Arc<AtomicBool>,

    load_info: LoadInfo,
    bytes_ready_metric: BytesCounterMetric,
    bytes_per_second_metric: BytesCounterMetric,
    sampling_tx: Option<oneshot::Sender<LoadInfo>>,
    load_info_sent_at: Arc<OnceLock<Instant>>,
}

impl LoadInfoDropHandler {
    fn report(mut self, peek: &RecordBatchPeek, bps: usize, rps: usize) {
        if self.omit.load(Ordering::Relaxed) {
            return;
        }

        self.set_per_col_bytes_ready(peek.per_col_bytes_ready());
        self.set_rows_ready(peek.rows_ready());
        self.set_per_col_ndv(peek.per_col_ndv());
        self.set_per_col_null_pct(peek.per_col_null_pct());
        self.set_per_col_bytes_per_second(bps);
        self.set_rows_per_second(rps)
    }

    fn set_per_col_bytes_ready(&mut self, bytes_ready: Vec<usize>) {
        self.load_info.per_column_bytes_ready = vec_cast(&bytes_ready);
        self.bytes_ready_metric.add_bytes(bytes_ready.iter().sum());
    }

    fn set_per_col_bytes_per_second(&mut self, total_bytes_per_second: usize) {
        let per_col_ready = &self.load_info.per_column_bytes_ready;
        let total_ready = per_col_ready.iter().sum::<usize>();
        let per_col: Vec<usize> = if total_ready == 0 {
            vec![total_bytes_per_second / per_col_ready.len().max(1); per_col_ready.len()]
        } else {
            per_col_ready
                .iter()
                .map(|&ready| ready.saturating_mul(total_bytes_per_second) / total_ready)
                .collect()
        };
        self.load_info.per_column_bytes_per_second = vec_cast(&per_col);
        self.bytes_per_second_metric
            .add_bytes(total_bytes_per_second);
    }

    fn set_rows_ready(&mut self, rows_ready: usize) {
        self.load_info.rows_ready = rows_ready;
    }

    fn set_rows_per_second(&mut self, rows_per_second: usize) {
        self.load_info.rows_per_second = rows_per_second;
    }

    fn set_per_col_ndv(&mut self, per_column_ndv: Vec<f32>) {
        self.load_info.per_column_ndv_percentage = per_column_ndv;
    }

    fn set_per_col_null_pct(&mut self, per_column_null_pct: Vec<f32>) {
        self.load_info.per_column_null_percentage = per_column_null_pct;
    }
}

impl Drop for LoadInfoDropHandler {
    fn drop(&mut self) {
        if self.omit.load(Ordering::Relaxed) {
            return;
        }
        if let Some(sampling_tx) = self.sampling_tx.take() {
            let _ = sampling_tx.send(std::mem::take(&mut self.load_info));
            let _ = self.load_info_sent_at.set(Instant::now());
        }
    }
}

fn zero_load_info(partition_idx: usize, n_cols: usize) -> LoadInfo {
    LoadInfo {
        partition: partition_idx,
        rows_per_second: 0,
        rows_ready: 0,
        per_column_bytes_per_second: vec![0; n_cols],
        per_column_bytes_ready: vec![0; n_cols],
        per_column_ndv_percentage: vec![0.0; n_cols],
        per_column_null_percentage: vec![0.0; n_cols],
    }
}

struct RecordBatchPeek {
    peek: VecDeque<RecordBatch>,
    n_cols: usize,
    max_batches_peeked: MaxGaugeMetric,
    max_mem_used: Gauge,
    memory_reservation: Arc<MemoryReservation>,
    first_batch_at: Arc<OnceLock<Instant>>,
}

impl RecordBatchPeek {
    fn push(&mut self, batch: RecordBatch) {
        let batch_size = record_batch_size(&batch);
        if self.peek.is_empty() {
            let _ = self.first_batch_at.set(Instant::now());
        }
        self.max_mem_used.add(batch_size);
        self.memory_reservation.grow(batch_size);
        self.peek.push_back(batch);
        self.max_batches_peeked.set_max(self.peek.len());
    }

    fn per_col_bytes_ready(&self) -> Vec<usize> {
        let mut result = vec![0; self.n_cols];
        for batch in self.peek.iter() {
            for (i, col) in batch.columns().iter().enumerate() {
                result[i] += column_size(col)
            }
        }
        result
    }

    fn rows_ready(&self) -> usize {
        self.peek.iter().map(|batch| batch.num_rows()).sum()
    }

    fn per_col_ndv(&self) -> Vec<f32> {
        let total_rows: usize = self.peek.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return vec![0.0; self.n_cols];
        }

        // Build the list of flat row indices to sample, sorted for cache-friendly access.
        let sampled_indices: Vec<usize> = if total_rows <= NDV_MAX_ROWS_SAMPLE {
            (0..total_rows).collect()
        } else {
            let mut indices =
                rand::seq::index::sample(&mut rand::rng(), total_rows, NDV_MAX_ROWS_SAMPLE)
                    .into_vec();
            indices.sort_unstable();
            indices
        };
        let rows_sampled = sampled_indices.len();

        let mut sets: Vec<HashSet<ScalarValue>> = vec![HashSet::new(); self.n_cols];
        let mut flat_base = 0usize;
        let mut sample_pos = 0usize;

        for batch in &self.peek {
            let batch_end = flat_base + batch.num_rows();
            while sample_pos < sampled_indices.len() && sampled_indices[sample_pos] < batch_end {
                let row = sampled_indices[sample_pos] - flat_base;
                for (col_idx, set) in sets.iter_mut().enumerate() {
                    let col = batch.column(col_idx);
                    if !col.is_null(row)
                        && let Ok(v) = ScalarValue::try_from_array(col, row)
                    {
                        set.insert(v);
                    }
                }
                sample_pos += 1;
            }
            if sample_pos >= sampled_indices.len() {
                break;
            }
            flat_base = batch_end;
        }

        sets.into_iter()
            .map(|s| s.len() as f32 / rows_sampled as f32)
            .collect()
    }

    fn per_col_null_pct(&self) -> Vec<f32> {
        let total_rows: usize = self.peek.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return vec![0.0; self.n_cols];
        }
        let mut counts = vec![0usize; self.n_cols];
        for batch in &self.peek {
            for (col_idx, count) in counts.iter_mut().enumerate() {
                *count += batch.column(col_idx).null_count();
            }
        }
        counts
            .into_iter()
            .map(|c| c as f32 / total_rows as f32)
            .collect()
    }
}

impl Stream for RecordBatchPeek {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_mut().peek.pop_front() {
            None => Poll::Ready(None),
            Some(batch) => {
                self.memory_reservation.shrink(record_batch_size(&batch));
                Poll::Ready(Some(Ok(batch)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.peek.len(), Some(self.peek.len()))
    }
}

fn column_size(arr: &ArrayRef) -> usize {
    arr.to_data().get_slice_memory_size().unwrap_or(0)
}

fn record_batch_size(batch: &RecordBatch) -> usize {
    batch.columns().iter().map(column_size).sum()
}

impl DisplayAs for SamplerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "SamplerExec: partitions={}",
            self.partition_samplers.len()
        )
    }
}

impl ExecutionPlan for SamplerExec {
    fn name(&self) -> &str {
        "SamplerExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(require_one_child(children)?)))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.execution_started.store(true, Ordering::Relaxed);
        let Some(partition_sampler) = self.partition_samplers.get(partition) else {
            return exec_err!("Partition {partition} not available in SamplerExec");
        };
        let Some(stream) = partition_sampler.start_stream() else {
            return exec_err!("SamplerExec[{partition}] was not kicked off");
        };
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric_set.clone_inner())
    }
}
