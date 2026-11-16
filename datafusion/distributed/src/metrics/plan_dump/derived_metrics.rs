//! Metrics that no node reports, but that are derived from the ones in a plan dump when it is
//! displayed, like the CPU time of a whole stage.

use crate::coordinator::ESTIMATED_OUTPUT_BYTES_METRIC;
use crate::metrics::plan_dump::plan_dump_display::{DecodedPlan, DecodedStage};
use datafusion::physical_expr_common::metrics::{CustomMetricValue, MetricValue};
use datafusion::physical_plan::metrics::{Metric, MetricsSet};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;

/// Derives metrics of a stage from the metrics of the stage, of its nodes and of the network
/// boundary reading it, if any. The metrics it returns are displayed along with the stage's own,
/// and there are none when the metrics they are derived from are missing.
type StageDerivedMetric = fn(&DecodedStage, Option<&MetricsSet>) -> Vec<Arc<Metric>>;

/// The metrics derived for every stage.
const STAGE_DERIVED_METRICS: &[StageDerivedMetric] = &[
    stage_elapsed_compute,
    stage_output_rows,
    stage_output_bytes,
    output_bytes_q_error,
];

/// The metrics derived for `stage`, read by a network boundary with `boundary_metrics`.
pub(super) fn stage_derived_metrics(
    stage: &DecodedStage,
    boundary_metrics: Option<&MetricsSet>,
) -> Vec<Arc<Metric>> {
    (STAGE_DERIVED_METRICS.iter())
        .flat_map(|derive| derive(stage, boundary_metrics))
        .collect()
}

/// `elapsed_compute`: the CPU time of a stage, which is that of all the nodes it runs. The nodes'
/// metrics keep their task, so they are added up per task when displayed.
fn stage_elapsed_compute(stage: &DecodedStage, _: Option<&MetricsSet>) -> Vec<Arc<Metric>> {
    (stage.plan.iter().flat_map(stage_plans))
        .flat_map(|plan| plan.metrics.iter())
        .filter(|metric| matches!(metric.value(), MetricValue::ElapsedCompute(_)))
        .cloned()
        .collect()
}

/// `output_rows`: the rows a stage returned. See [stage_output].
fn stage_output_rows(
    stage: &DecodedStage,
    boundary_metrics: Option<&MetricsSet>,
) -> Vec<Arc<Metric>> {
    stage_output(stage, boundary_metrics, "output_rows")
}

/// `output_bytes`: the bytes a stage returned. See [stage_output].
fn stage_output_bytes(
    stage: &DecodedStage,
    boundary_metrics: Option<&MetricsSet>,
) -> Vec<Arc<Metric>> {
    stage_output(stage, boundary_metrics, "output_bytes")
}

/// The `name` metrics of what a stage returned, added up across tasks. For a stage read by
/// another one, they are the ones of the network boundary reading it, which are what crossed the
/// network: the stage's own root, like the `RepartitionExec` of a shuffle, can report many more
/// bytes, as the batches it produces may be slices of bigger ones. They are labeled by the tasks
/// of the stage reading it, so they are only displayed in total. The head stage returns what its
/// root returns.
fn stage_output(
    stage: &DecodedStage,
    boundary_metrics: Option<&MetricsSet>,
    name: &str,
) -> Vec<Arc<Metric>> {
    let root_metrics = stage.plan.as_ref().map(|plan| &plan.metrics);
    let Some(metrics) = boundary_metrics.or(root_metrics) else {
        return vec![];
    };
    (metrics.iter())
        .filter(|metric| metric.value().name() == name)
        .map(|metric| Arc::new(Metric::new(metric.value().clone(), None)))
        .collect()
}

/// `output_bytes_q_error`: how far the bytes a stage was estimated to return are from the bytes it
/// returned, as the ratio of the larger to the smaller, so over and underestimates by the same
/// factor are equally bad. Only stages whose output is estimated, which happens with dynamic task
/// counts, have it.
/// The bytes a stage returned are the ones of [stage_output_bytes].
fn output_bytes_q_error(
    stage: &DecodedStage,
    boundary_metrics: Option<&MetricsSet>,
) -> Vec<Arc<Metric>> {
    let Some(estimated) = metric_total(&stage.metrics, ESTIMATED_OUTPUT_BYTES_METRIC) else {
        return vec![];
    };
    let output_bytes = stage_output_bytes(stage, boundary_metrics);
    if output_bytes.is_empty() {
        return vec![];
    }
    let actual: usize = (output_bytes.iter())
        .map(|metric| metric.value().as_usize())
        .sum();
    let (estimated, actual) = (estimated.max(1) as f64, actual.max(1) as f64);
    let q_error = (estimated / actual).max(actual / estimated);
    vec![QErrorMetric::new_metric("output_bytes_q_error", q_error)]
}

/// The nodes a stage runs, which do not include the ones of the stages it reads from.
fn stage_plans(plan: &DecodedPlan) -> Vec<&DecodedPlan> {
    let mut plans = vec![plan];
    for child in &plan.children {
        plans.extend(stage_plans(child));
    }
    plans
}

fn metric_total(metrics: &MetricsSet, name: &str) -> Option<usize> {
    let total = metrics.sum(|metric| metric.value().name() == name)?;
    Some(total.as_usize())
}

/// A factor by which an estimate is off, displayed like `2.00`. Aggregating several keeps the
/// worst one.
#[derive(Debug, Default)]
struct QErrorMetric {
    /// The bits of the `f64` factor.
    bits: AtomicU64,
}

impl QErrorMetric {
    fn new_metric(name: &'static str, q_error: f64) -> Arc<Metric> {
        let value = QErrorMetric {
            bits: AtomicU64::new(q_error.to_bits()),
        };
        let value = MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value),
        };
        Arc::new(Metric::new(value, None))
    }

    fn value(&self) -> f64 {
        f64::from_bits(self.bits.load(Relaxed))
    }
}

impl Display for QErrorMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.2}", self.value())
    }
}

impl CustomMetricValue for QErrorMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(QErrorMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        let worst = self.value().max(other.value());
        self.bits.store(worst.to_bits(), Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.value() as usize
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        other.value() == self.value()
    }
}
