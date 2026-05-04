use crate::common::require_one_child;
use crate::worker::generated::worker as pb;
use crate::{LatencyMetricExt, MaxLatencyMetric, P50LatencyMetric};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_err};
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::{Gauge, MetricsSet};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;

#[derive(Debug)]
pub struct SamplerExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) metric_set: ExecutionPlanMetricsSet,
    pub(crate) partition_samplers: Vec<PartitionSampler>,
}

/// Metrics that quantify how long the sampler held data in memory before the consumer
/// (real execution) attached, plus the peak buffer size reached. All metrics are shared
/// across the partition samplers; the latency metrics aggregate per-partition observations.
#[derive(Debug, Clone)]
pub(crate) struct SamplerExecMetrics {
    /// Time from `kick_off()` (when the producer task starts pulling the input plan) until
    /// `execute()` is invoked on this partition (when the consumer attaches).
    kickoff_to_execution_p50: P50LatencyMetric,
    kickoff_to_execution_max: MaxLatencyMetric,
    /// Time from the first `LoadInfo` message being emitted until `execute()` is invoked.
    /// Measures how long the coordinator sat between seeing the first sample and starting
    /// the consumer.
    first_load_info_to_execution_p50: P50LatencyMetric,
    first_load_info_to_execution_max: MaxLatencyMetric,
    /// Peak memory buffered by any partition sampler during the sampling phase.
    max_mem_used: Gauge,
}

impl SamplerExecMetrics {
    fn new(metric_set: &ExecutionPlanMetricsSet) -> Self {
        Self {
            kickoff_to_execution_p50: MetricBuilder::new(metric_set)
                .p50_latency("kickoff_to_execution_p50"),
            kickoff_to_execution_max: MetricBuilder::new(metric_set)
                .max_latency("kickoff_to_execution_max"),
            first_load_info_to_execution_p50: MetricBuilder::new(metric_set)
                .p50_latency("first_load_info_to_execution_p50"),
            first_load_info_to_execution_max: MetricBuilder::new(metric_set)
                .max_latency("first_load_info_to_execution_max"),
            max_mem_used: MetricBuilder::new(metric_set).global_gauge("max_mem_used"),
        }
    }
}

impl SamplerExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let metric_set = ExecutionPlanMetricsSet::new();
        let metrics = SamplerExecMetrics::new(&metric_set);
        let partitions = input.properties().partitioning.partition_count();
        let mut samplers = Vec::with_capacity(partitions);
        for i in 0..partitions {
            samplers.push(PartitionSampler {
                partition_idx: i,
                input: Arc::clone(&input),
                stream: Mutex::new(None),
                execution_flag: Arc::new(AtomicBool::new(false)),
                metrics: metrics.clone(),
                kick_off_at: Arc::new(OnceLock::new()),
                first_load_info_at: Arc::new(OnceLock::new()),
            });
        }
        Self {
            input,
            metric_set,
            partition_samplers: samplers,
        }
    }

    pub(crate) fn kick_off_first_sampler(
        plan: Arc<dyn ExecutionPlan>,
        tx: &UnboundedSender<pb::LoadInfo>,
        ctx: Arc<TaskContext>,
    ) -> Result<()> {
        plan.apply(|plan| {
            let Some(sampler) = plan.as_any().downcast_ref::<SamplerExec>() else {
                return Ok(TreeNodeRecursion::Continue);
            };
            for partition_sampler in &sampler.partition_samplers {
                partition_sampler.kick_off(tx.clone(), Arc::clone(&ctx))?;
            }
            Ok(TreeNodeRecursion::Stop)
        })?;
        Ok(())
    }
}

pub(crate) struct PartitionSampler {
    partition_idx: usize,
    input: Arc<dyn ExecutionPlan>,
    stream: Mutex<Option<SendableRecordBatchStream>>,
    execution_flag: Arc<AtomicBool>,

    // Metrics state.
    metrics: SamplerExecMetrics,
    /// Set when `kick_off` is invoked. Used at `execute()` time to record how long the
    /// sampler buffered data before the consumer attached.
    kick_off_at: Arc<OnceLock<Instant>>,
    /// Set the first time the producer task emits a `LoadInfo`. Used at `execute()` time
    /// to record the gap between the first sample and the consumer starting.
    first_load_info_at: Arc<OnceLock<Instant>>,
}

impl Debug for PartitionSampler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionSampler").finish()
    }
}

impl PartitionSampler {
    fn start_stream(&self) -> Option<SendableRecordBatchStream> {
        self.execution_flag.store(true, Ordering::SeqCst);
        let now = Instant::now();
        if let Some(t) = self.kick_off_at.get() {
            let delay = now.saturating_duration_since(*t);
            self.metrics.kickoff_to_execution_p50.add_duration(delay);
            self.metrics.kickoff_to_execution_max.add_duration(delay);
        }
        if let Some(t) = self.first_load_info_at.get() {
            let delay = now.saturating_duration_since(*t);
            self.metrics
                .first_load_info_to_execution_p50
                .add_duration(delay);
            self.metrics
                .first_load_info_to_execution_max
                .add_duration(delay);
        }
        self.stream.lock().unwrap().take()
    }

    fn kick_off(
        &self,
        sampling_tx: UnboundedSender<pb::LoadInfo>,
        ctx: Arc<TaskContext>,
    ) -> Result<()> {
        let _ = self.kick_off_at.set(Instant::now());

        let input = Arc::clone(&self.input);
        let partition_idx = self.partition_idx;
        let schema = input.schema();

        let memory_reservation = Arc::new(
            MemoryConsumer::new(format!("PartitionSampler[{partition_idx}]"))
                .register(ctx.memory_pool()),
        );
        let memory_reservation_for_consumer = Arc::clone(&memory_reservation);

        // Producer pauses when the buffer exceeds the budget; consumers wake it
        // via this Notify after each shrink. Without the gate, the unbounded
        // queue could grow without bound while the next stage hasn't started.
        let mem_available_notify = Arc::new(Notify::new());
        let mem_available_notify_for_consumer = Arc::clone(&mem_available_notify);

        let execution_flag = Arc::clone(&self.execution_flag);
        let max_mem_used = self.metrics.max_mem_used.clone();
        let first_load_info_at = Arc::clone(&self.first_load_info_at);

        // Execute the input synchronously so any setup error surfaces before we
        // spawn the producer task.
        let mut input_stream = input.execute(partition_idx, ctx)?;

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<Result<RecordBatch>>();

        let task = SpawnedTask::spawn(async move {
            let mut first_msg_ns = None;
            let mut max_mem_signaled = false;

            while let Some(batch_or_err) = input_stream.next().await {
                let batch = match batch_or_err {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.send(Err(e));
                        return;
                    }
                };
                let size = batch.get_array_memory_size();

                // Backpressure: pause once the buffer exceeds the budget. The
                // consumer (real execution) wakes us after each batch it drains.
                while memory_reservation.size() >= PARTITION_SAMPLER_BUDGET_BYTES {
                    mem_available_notify.notified().await;
                }

                memory_reservation.grow(size);
                max_mem_used.set_max(memory_reservation.size());

                if !execution_flag.load(Ordering::Relaxed) {
                    let time = Instant::now();
                    let time_mark_ns = match first_msg_ns {
                        Some(t) => time - t,
                        None => {
                            first_msg_ns = Some(time);
                            let _ = first_load_info_at.set(time);
                            Duration::default()
                        }
                    };
                    let max_memory_reached = !max_mem_signaled
                        && memory_reservation.size() >= PARTITION_SAMPLER_BUDGET_BYTES;
                    if max_memory_reached {
                        max_mem_signaled = true;
                    }
                    let _ = sampling_tx.send(pb::LoadInfo {
                        partition: partition_idx as u64,
                        row_count: batch.num_rows() as u64,
                        byte_size: size as u64,
                        time_mark_ns: time_mark_ns.as_nanos() as u64,
                        eos: false,
                        max_memory_reached,
                    });
                }

                if tx.send(Ok(batch)).is_err() {
                    return;
                }
            }

            // End of input: if real execution hasn't started yet, tell the
            // coordinator we observed the entire stream.
            if !execution_flag.load(Ordering::Relaxed) {
                let time_mark_ns = first_msg_ns.map(|t| Instant::now() - t).unwrap_or_default();
                let _ = sampling_tx.send(pb::LoadInfo {
                    partition: partition_idx as u64,
                    row_count: 0,
                    byte_size: 0,
                    time_mark_ns: time_mark_ns.as_nanos() as u64,
                    eos: true,
                    max_memory_reached: false,
                });
            }
        });

        let stream = UnboundedReceiverStream::new(rx).map(move |result| {
            let _ = &task; // keep the task alive as long as the stream is alive.
            if let Ok(batch) = &result {
                memory_reservation_for_consumer.shrink(batch.get_array_memory_size());
                mem_available_notify_for_consumer.notify_one();
            }
            result
        });
        let stream = RecordBatchStreamAdapter::new(schema, stream);

        self.stream
            .lock()
            .expect("poisoned lock")
            .replace(Box::pin(stream));

        Ok(())
    }
}

/// Soft byte budget the partition sampler will buffer before pausing the
/// producer. Once exceeded, a `max_memory_reached` LoadInfo is emitted once,
/// signaling to the coordinator that the producer can sustain at least this
/// much in-flight data.
/// TODO: make this configurable via DistributedConfig.
const PARTITION_SAMPLER_BUDGET_BYTES: usize = 32 * 1024 * 1024;

impl DisplayAs for SamplerExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SamplerExec")
    }
}

impl ExecutionPlan for SamplerExec {
    fn name(&self) -> &str {
        "SamplerExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
        let Some(stream) = self.partition_samplers[partition].start_stream() else {
            return exec_err!("SamplerExec[{partition}] was not kicked off");
        };
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric_set.clone_inner())
    }
}
