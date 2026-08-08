use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion_distributed::{NetworkBoundaryExt, QErrorMetric, STATS_Q_ERROR_METRIC, Stage};
use sketches_ddsketch::{Config, DDSketch};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StatsEstimationQError {
    pub p50: f64,
    pub p95: f64,
}

/// Collects P50 and P95 of the q-error recorded at every dynamically sampled stage boundary in
/// `plan`.
pub fn stats_estimation_q_error(plan: &Arc<dyn ExecutionPlan>) -> Option<StatsEstimationQError> {
    let mut boundary_q_errors = DDSketch::new(Config::defaults());

    let _ = plan.apply(|node| {
        if let Some(boundary) = node.as_network_boundary()
            && let Stage::Local(input_stage) = boundary.input_stage()
            && let Some(q_error) = q_error_metric_value(&input_stage.metrics_set)
        {
            boundary_q_errors.add(q_error);
        }
        Ok(TreeNodeRecursion::Continue)
    });

    q_error_percentiles(&boundary_q_errors)
}

fn q_error_percentiles(q_errors: &DDSketch) -> Option<StatsEstimationQError> {
    Some(StatsEstimationQError {
        p50: q_errors.quantile(0.50).ok().flatten()?,
        p95: q_errors.quantile(0.95).ok().flatten()?,
    })
}

fn q_error_metric_value(metrics: &MetricsSet) -> Option<f64> {
    metrics.iter().find_map(|metric| match metric.value() {
        MetricValue::Custom { name, value } if name == STATS_Q_ERROR_METRIC => value
            .as_any()
            .downcast_ref::<QErrorMetric>()
            .map(QErrorMetric::value),
        _ => None,
    })
}

pub fn median(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable_by(f64::total_cmp);
    let mid = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q_error_percentiles_returns_none_for_an_empty_sketch() {
        assert_eq!(
            q_error_percentiles(&DDSketch::new(Config::defaults())),
            None
        );
    }

    #[test]
    fn q_error_percentiles_reports_regular_and_tail_cases() {
        let mut sketch = DDSketch::new(Config::defaults());
        for value in 1..=100 {
            sketch.add(value as f64);
        }

        let percentiles = q_error_percentiles(&sketch).unwrap();
        assert!((49.0..=51.0).contains(&percentiles.p50));
        assert!((94.0..=96.0).contains(&percentiles.p95));
    }

    #[test]
    fn reads_q_error_metric_value() {
        let mut metrics = MetricsSet::new();
        metrics.push(QErrorMetric::new_metric(STATS_Q_ERROR_METRIC, 2.75));

        assert_eq!(q_error_metric_value(&metrics), Some(2.75));
    }
}
