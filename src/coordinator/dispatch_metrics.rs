use crate::common::now_ns;
use crate::coordinator::latency_metric::LatencyMetric;
use crate::{BytesCounterMetric, BytesMetricExt, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL};
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, Label, MetricBuilder};
use std::sync::Arc;

/// Metrics that measure network details about communications between [crate::DistributedExec] and a
/// worker. Shared by every transport's dispatch path, so the plan-send byte/latency counters read
/// the same regardless of how plans reach the workers.
#[derive(Clone)]
pub struct CoordinatorToWorkerMetrics {
    pub plan_bytes_sent: BytesCounterMetric,
    pub plan_send_latency: Arc<LatencyMetric>,
    pub instantiation_time: u64,
}

impl CoordinatorToWorkerMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            // Metric that measures to total sum of bytes worth of subplans sent.
            plan_bytes_sent: MetricBuilder::new(metrics)
                .with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0"))
                .bytes_counter("plan_bytes_sent"),
            // Latency statistics about the network calls issued to the workers for feeding subplans.
            plan_send_latency: Arc::new(LatencyMetric::new(
                "plan_send_latency",
                |b| b.with_label(Label::new(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, "0")),
                metrics,
            )),
            instantiation_time: now_ns(),
        }
    }
}
