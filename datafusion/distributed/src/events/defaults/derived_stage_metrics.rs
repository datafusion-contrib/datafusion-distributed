use crate::{TaskFinishedEvent, TaskFinishedEventResponse};
use datafusion::common::Result;
use datafusion::common::format::MetricCategory;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_expr_common::metrics::MetricValue;
use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::{MetricType, MetricsSet, Time};
use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

const STAGE_ELAPSED_COMPUTE_METRIC: &str = "stage_elapsed_compute";

pub(crate) fn derived_stage_metrics(ev: TaskFinishedEvent) -> Result<TaskFinishedEventResponse> {
    let stage_elapsed_compute = Time::new();
    let mut metric_set = MetricsSet::new();
    metric_set.push(Arc::new(
        Metric::new(
            MetricValue::Time {
                name: Cow::Borrowed(STAGE_ELAPSED_COMPUTE_METRIC),
                time: stage_elapsed_compute.clone(),
            },
            None,
        )
        .with_type(MetricType::Dev)
        .with_category(MetricCategory::Timing),
    ));

    ev.plan.apply(|plan| {
        let Some(metrics) = plan.metrics() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        if let Some(elapsed_compute) = metrics.elapsed_compute() {
            stage_elapsed_compute.add_duration(Duration::from_nanos(elapsed_compute as u64));
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(TaskFinishedEventResponse::new(metric_set))
}
