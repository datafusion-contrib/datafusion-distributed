use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion_distributed::{NetworkBoundaryExt, Stage};
use sketches_ddsketch::{Config, DDSketch};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StatsEstimationQError {
    pub p50: f64,
    pub p95: f64,
}

/// Computes P50 and P95 q-error between the sampled byte estimate and the actual output bytes for
/// every dynamically sampled stage boundary in `plan`.
pub fn stats_estimation_q_error(plan: &Arc<dyn ExecutionPlan>) -> Option<StatsEstimationQError> {
    let mut boundary_q_errors = DDSketch::new(Config::defaults());

    let _ = plan.apply(|node| {
        if let Some(boundary) = node.as_network_boundary()
            && let Stage::Local(input_stage) = boundary.input_stage()
            && let Some(sampled_bytes) = metric_total(&input_stage.metrics_set, "sampled_bytes")
            && let Some(actual_bytes) = node
                .metrics()
                .and_then(|metrics| metric_total(&metrics, "output_bytes"))
        {
            boundary_q_errors.add(q_error(sampled_bytes, actual_bytes));
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

fn metric_total(metrics: &MetricsSet, name: &str) -> Option<usize> {
    metrics
        .sum(|metric| metric.value().name() == name)
        .map(|value| value.as_usize())
}

/// Q-error is the standard cardinality-estimation metric because it treats equal-factor over- and
/// underestimates symmetrically. See https://www.vldb.org/pvldb/vol2/vldb09-657.pdf and
/// https://vldb.org/pvldb/vol9/p204-leis.pdf.
fn q_error(estimated: usize, actual: usize) -> f64 {
    let estimated = estimated.max(1) as f64;
    let actual = actual.max(1) as f64;
    (estimated / actual).max(actual / estimated)
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
}
