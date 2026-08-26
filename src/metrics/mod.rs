mod bytes_metric;
mod latency_metric;
mod max_gauge_metric;
mod task_metrics_collector;
mod task_metrics_rewriter;

pub use bytes_metric::{BytesCounterMetric, BytesMetricExt};
pub use latency_metric::{
    AvgLatencyMetric, FirstLatencyMetric, LatencyMetricExt, MaxLatencyMetric, MinLatencyMetric,
    P50LatencyMetric, P75LatencyMetric, P95LatencyMetric, P99LatencyMetric,
};
pub use max_gauge_metric::{GaugeMetricExt, MaxGaugeMetric};
pub(crate) use task_metrics_collector::collect_plan_metrics;
pub use task_metrics_rewriter::{DistributedMetricsFormat, rewrite_distributed_plan_with_metrics};

/// Emitted by dynamic-planner stage records; estimates the CPU cost of the stage input.
pub const CPU_COST_METRIC: &str = "cpu_cost";
/// Emitted by dynamic-planner stage records; estimates the memory cost of the stage input.
pub const MEMORY_COST_METRIC: &str = "memory_cost";
/// Emitted by dynamic-planner stage records; estimates the network cost of the stage input.
pub const NETWORK_COST_METRIC: &str = "network_cost";
/// Emitted by dynamic-planner stage records; estimates the percentage of input sampled.
pub const ESTIMATED_PCT_SAMPLED_METRIC: &str = "estimated_pct_sampled";
/// Emitted by dynamic-planner stage records; estimates the stage's total output size in bytes.
pub const ESTIMATED_OUTPUT_BYTES_METRIC: &str = "estimated_output_bytes";
/// Emitted by `DistributedExec`; counts coordinator-to-worker channels routed locally.
pub const LOCAL_COORDINATOR_CHANNELS_METRIC: &str = "local_coordinator_channels";
/// Emitted by `DistributedExec`; counts coordinator-to-worker channels routed remotely.
pub const REMOTE_COORDINATOR_CHANNELS_METRIC: &str = "remote_coordinator_channels";
/// Emitted by `DistributedExec`; measures latency for sending a plan to a worker.
pub const PLAN_SEND_LATENCY_METRIC: &str = "plan_send_latency";
/// Emitted by coordinator-to-worker streams; counts encoded plan bytes sent to workers.
pub const PLAN_BYTES_SENT_METRIC: &str = "plan_bytes_sent";
/// Emitted by worker task data; records when a coordinator added the task plan.
pub const PLAN_ADDED_AT_METRIC: &str = "plan_added_at";
/// Emitted by worker task data; records when the worker began executing the task plan.
pub const PLAN_EXECUTED_AT_METRIC: &str = "plan_executed_at";
/// Emitted by worker task data; records when the worker finished the task plan's stream.
pub const PLAN_FINISHED_AT_METRIC: &str = "plan_finished_at";

/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Counts encoded record-batch bytes received from workers.
pub const BYTES_TRANSFERRED_METRIC: &str = "bytes_transferred";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, `NetworkBroadcastExec`, and `SamplerExec`.
/// Records peak buffered memory in bytes.
pub const MAX_MEMORY_USED_METRIC: &str = "max_mem_used";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Counts messages received from workers.
pub const MESSAGE_COUNT_METRIC: &str = "msg_count";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Records the minimum worker-message latency.
pub const NETWORK_LATENCY_MIN_METRIC: &str = "network_latency_min";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Records the maximum worker-message latency.
pub const NETWORK_LATENCY_MAX_METRIC: &str = "network_latency_max";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Records the 50th-percentile worker-message latency.
pub const NETWORK_LATENCY_P50_METRIC: &str = "network_latency_p50";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Records the 95th-percentile worker-message latency.
pub const NETWORK_LATENCY_P95_METRIC: &str = "network_latency_p95";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Records latency of the first worker message.
pub const NETWORK_LATENCY_FIRST_METRIC: &str = "network_latency_first";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Sums worker-message latencies.
pub const NETWORK_LATENCY_SUM_METRIC: &str = "network_latency_sum";
/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Counts worker messages included in latency metrics.
pub const NETWORK_LATENCY_COUNT_METRIC: &str = "network_latency_count";

/// Emitted by `SamplerExec`; records P50 time from sampler kickoff to its first batch.
pub const KICK_OFF_TO_FIRST_BATCH_P50_METRIC: &str = "kick_off_to_first_batch_p50";
/// Emitted by `SamplerExec`; records maximum time from sampler kickoff to its first batch.
pub const KICK_OFF_TO_FIRST_BATCH_MAX_METRIC: &str = "kick_off_to_first_batch_max";
/// Emitted by `SamplerExec`; records P50 time from kickoff to sending load information.
pub const KICK_OFF_TO_LOAD_INFO_SENT_P50_METRIC: &str = "kick_off_to_load_info_sent_p50";
/// Emitted by `SamplerExec`; records maximum time from kickoff to sending load information.
pub const KICK_OFF_TO_LOAD_INFO_SENT_MAX_METRIC: &str = "kick_off_to_load_info_sent_max";
/// Emitted by `SamplerExec`; records P50 time from kickoff to execution.
pub const KICK_OFF_TO_EXECUTION_P50_METRIC: &str = "kick_off_to_execution_p50";
/// Emitted by `SamplerExec`; records maximum time from kickoff to execution.
pub const KICK_OFF_TO_EXECUTION_MAX_METRIC: &str = "kick_off_to_execution_max";
/// Emitted by `SamplerExec`; records the largest number of record batches held for sampling.
pub const MAX_BATCHES_PEEKED_METRIC: &str = "max_batches_peeked";
/// Emitted by `SamplerExec`; counts bytes ready when it reports load information.
pub const BYTES_READY_METRIC: &str = "bytes_ready";

/// Emitted by `NetworkCoalesceExec`, `NetworkShuffleExec`, and `NetworkBroadcastExec`.
/// Counts worker connections resolved to the local process.
pub const LOCAL_CONNECTIONS_USED_METRIC: &str = "local_connections_used";
/// Emitted by `RemoteFeedProvider`; counts encoded work-unit bytes received from the coordinator.
pub const WORK_UNIT_BYTES_METRIC: &str = "work_unit_bytes";
/// Emitted by `RemoteFeedProvider`; counts work units delivered in-memory rather than over transport.
pub const WORK_UNIT_IN_MEMORY_COUNT_METRIC: &str = "work_unit_in_memory_count";
/// Emitted by `RemoteFeedProvider`; counts work units received from the coordinator.
pub const WORK_UNIT_COUNT_METRIC: &str = "work_unit_count";
/// Emitted by `RemoteFeedProvider`; records maximum coordinator-to-worker work-unit send latency.
pub const WORK_UNIT_SEND_LATENCY_MAX_METRIC: &str = "work_unit_send_latency_max";
/// Emitted by `RemoteFeedProvider`; records P50 coordinator-to-worker work-unit send latency.
pub const WORK_UNIT_SEND_LATENCY_P50_METRIC: &str = "work_unit_send_latency_p50";
/// Emitted by `RemoteFeedProvider`; records maximum work-unit receive latency.
pub const WORK_UNIT_RECEIVED_LATENCY_MAX_METRIC: &str = "work_unit_received_latency_max";
/// Emitted by `RemoteFeedProvider`; records P50 work-unit receive latency.
pub const WORK_UNIT_RECEIVED_LATENCY_P50_METRIC: &str = "work_unit_received_latency_p50";
/// Emitted by `RemoteFeedProvider`; records maximum work-unit processing latency.
pub const WORK_UNIT_PROCESSED_LATENCY_MAX_METRIC: &str = "work_unit_processed_latency_max";
/// Emitted by `RemoteFeedProvider`; records P50 work-unit processing latency.
pub const WORK_UNIT_PROCESSED_LATENCY_P50_METRIC: &str = "work_unit_processed_latency_p50";

/// Label used to annotate metrics in execution plan nodes with the task in which they were executed.
/// Note that the same task id may be used in multiple stages.
pub const DISTRIBUTED_DATAFUSION_TASK_ID_LABEL: &str = "task_id";
