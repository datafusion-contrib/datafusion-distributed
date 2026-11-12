mod aggregate_by_name;
mod bytes_metric;
pub(crate) mod generated;
mod latency_metric;
mod max_gauge_metric;
mod metrics_proto;
mod task_metrics_collector;
mod task_metrics_rewriter;

pub(crate) use aggregate_by_name::aggregate_by_name;
pub use bytes_metric::{BytesCounterMetric, BytesMetricExt};
pub use latency_metric::{
    AvgLatencyMetric, FirstLatencyMetric, LatencyMetricExt, MaxLatencyMetric, MinLatencyMetric,
    P50LatencyMetric, P75LatencyMetric, P95LatencyMetric, P99LatencyMetric,
};
pub use max_gauge_metric::{GaugeMetricExt, MaxGaugeMetric};
pub(crate) use metrics_proto::{df_metrics_set_to_proto, metrics_set_proto_to_df};
pub(crate) use task_metrics_collector::collect_plan_metrics;
pub use task_metrics_rewriter::{DistributedMetricsFormat, rewrite_distributed_plan_with_metrics};
/// Label used to annotate metrics in execution plan nodes with the task in which they were executed.
/// Note that the same task id may be used in multiple stages.
pub const DISTRIBUTED_DATAFUSION_TASK_ID_LABEL: &str = "task_id";
