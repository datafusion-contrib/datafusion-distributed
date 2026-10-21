use crate::{
    BytesCounterMetric, DistributedConfig, StageBuiltEvent, StageBuiltEventResponse,
    TaskCountAnnotation,
};
use datafusion::common::Result;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlanProperties;

pub(crate) fn cost_based_stage_built_event_handler(
    ev: StageBuiltEvent,
) -> Result<StageBuiltEventResponse> {
    let d_cfg = DistributedConfig::from_session_config(ev.session_config)?;
    let partitions = ev.plan.output_partitioning().partition_count();
    let compute_based_task_count = *ev.cost.cpu.get_value().unwrap_or(&0) as f64
        / d_cfg.dynamic_bytes_per_partition.max(1) as f64
        / partitions.max(1) as f64;

    let mut metrics = MetricsSet::new();
    if let Some(v) = ev.cost.cpu.get_value() {
        metrics.push(BytesCounterMetric::new_metric("cpu_cost", *v));
    }
    if let Some(v) = ev.cost.memory.get_value() {
        metrics.push(BytesCounterMetric::new_metric("memory_cost", *v));
    }
    if let Some(v) = ev.cost.network.get_value() {
        metrics.push(BytesCounterMetric::new_metric("network_cost", *v));
    }

    Ok(StageBuiltEventResponse::new(ev.plan)
        .with_task_count(TaskCountAnnotation::soft(compute_based_task_count))
        .with_metrics(metrics))
}
