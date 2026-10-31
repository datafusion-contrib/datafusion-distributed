use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use std::any::TypeId;
use std::mem::{Discriminant, discriminant};
use std::sync::Arc;

use datafusion::common::HashSet;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use delegate::delegate;
use std::fmt::{Debug, Formatter};

/// A transparent wrapper that delegates all execution to its child but returns custom metrics. This node is invisible during display.
/// The structure of a plan tree is closely tied to the [TaskMetricsRewriter].
pub(crate) struct MetricsWrapperExec {
    inner: Arc<dyn ExecutionPlan>,
    /// metrics for this plan node.
    metrics: MetricsSet,
}

impl MetricsWrapperExec {
    pub(crate) fn new(inner: Arc<dyn ExecutionPlan>, metrics: MetricsSet) -> Self {
        Self { inner, metrics }
    }

    #[cfg(test)]
    pub(crate) fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

/// MetricsWrapperExec is invisible during display.
impl DisplayAs for MetricsWrapperExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

/// MetricsWrapperExec is visible when debugging.
impl Debug for MetricsWrapperExec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MetricsWrapperExec ({:?})", self.inner)
    }
}

impl ExecutionPlan for MetricsWrapperExec {
    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn properties(&self) -> &Arc<PlanProperties>;
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MetricsWrapperExec {
            inner: Arc::clone(&self.inner).replace_children(
                children.clone(),
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("MetricsWrapperExec does not implement execute")
    }

    /// The supplied snapshot is authoritative for each reported metric family,
    /// including all partitions. Preserve local-only families, such as counters
    /// on a coordinator's work-unit feed that cannot be reported by workers.
    fn metrics(&self) -> Option<MetricsSet> {
        let mut metrics = self.metrics.clone();
        if let Some(local_metrics) = self.inner.metrics() {
            let mut reported = HashSet::new();
            for metric in self.metrics.iter() {
                reported.insert(metric_family(metric.value()));
            }
            for local in local_metrics {
                if !reported.contains(&metric_family(local.value())) {
                    metrics.push(local);
                }
            }
        }
        Some(metrics)
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }
}

fn metric_family(value: &MetricValue) -> (Discriminant<MetricValue>, &str, Option<TypeId>) {
    let custom_type = match value {
        MetricValue::Custom { value, .. } => Some(value.as_any().type_id()),
        _ => None,
    };
    (discriminant(value), value.name(), custom_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{BytesCounterMetric, MaxGaugeMetric};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::{Count, Metric};
    use std::hint::black_box;
    use std::time::Instant;

    #[test]
    fn supplied_snapshot_is_authoritative_for_in_process_workers() {
        let raw = counters(3);
        for snapshot in [raw.clone(), counters(1)] {
            let expected = snapshot.iter().cloned().collect::<Vec<_>>();
            let wrapper = wrapper(raw.clone(), snapshot);
            let actual = wrapper.metrics().unwrap();
            assert_eq!(actual.iter().count(), expected.len());
            for (actual, expected) in actual.iter().zip(expected) {
                assert!(Arc::ptr_eq(actual, &expected));
            }
        }
    }

    #[test]
    fn local_only_metric_families_are_preserved() {
        let mut local = counters(3);
        let feed_count = Count::new();
        feed_count.add(6);
        local.push(Arc::new(Metric::new(
            MetricValue::Count {
                name: "work_units_sent".into(),
                count: feed_count,
            },
            Some(0),
        )));
        let metrics = wrapper(local.clone(), counters(3).aggregate_by_name())
            .metrics()
            .unwrap();
        assert_eq!(metrics.iter().count(), 2);
        assert_eq!(
            metrics.sum_by_name("work_units_sent").unwrap().as_usize(),
            6
        );
        assert_eq!(
            wrapper(local, MetricsSet::new())
                .metrics()
                .unwrap()
                .iter()
                .count(),
            4
        );
    }

    #[test]
    fn metric_families_distinguish_value_variants_and_custom_types() {
        let mut local = MetricsSet::new();
        local.push(Arc::new(Metric::new(
            MetricValue::Count {
                name: "output_rows".into(),
                count: Count::new(),
            },
            None,
        )));
        local.push(BytesCounterMetric::new_metric("custom", 1));
        let mut snapshot = counters(1);
        snapshot.push(MaxGaugeMetric::new_metric("custom", 1));
        let metrics = wrapper(local.clone(), snapshot).metrics().unwrap();
        assert_eq!(metrics.iter().count(), 4);
        for (actual, expected) in metrics.iter().skip(2).zip(local.iter()) {
            assert!(Arc::ptr_eq(actual, expected));
        }
    }

    #[test]
    fn compact_snapshot_does_not_reintroduce_raw_partition_metrics() {
        let raw = counters(3);
        let summary = raw.aggregate_by_name();
        let wrapper = wrapper(raw, summary);
        let metrics = wrapper.metrics().unwrap();
        assert_eq!(metrics.iter().count(), 1);
        let metric = metrics.iter().next().unwrap();
        assert_eq!(metric.value().as_usize(), 6);
        assert_eq!(metric.partition(), None);
    }

    #[test]
    #[ignore = "terminal metrics microbenchmark; run with --release --ignored --nocapture"]
    fn benchmark_terminal_snapshot() {
        for entries in [4_096, 16_384, 65_536] {
            let metrics = counters(entries);
            let wrapper = wrapper(metrics.clone(), metrics);
            let mut legacy = Vec::new();
            let mut snapshot = Vec::new();
            for iteration in 0..4 {
                let run_legacy = || {
                    let start = Instant::now();
                    drop(black_box(legacy_metrics(black_box(&wrapper))));
                    start.elapsed()
                };
                let run_snapshot = || {
                    let start = Instant::now();
                    for _ in 0..100 {
                        drop(black_box(black_box(&wrapper).metrics()));
                    }
                    start.elapsed() / 100
                };
                let (before, after) = if iteration % 2 == 0 {
                    (run_legacy(), run_snapshot())
                } else {
                    let after = run_snapshot();
                    (run_legacy(), after)
                };
                if iteration > 0 {
                    legacy.push(before);
                    snapshot.push(after);
                }
            }
            legacy.sort();
            snapshot.sort();
            let before = legacy[1];
            let after = snapshot[1];
            eprintln!(
                "entries={entries} legacy_ms={:.3} snapshot_ms={:.3} speedup={:.1}x (includes result clone/drop)",
                before.as_secs_f64() * 1000.0,
                after.as_secs_f64() * 1000.0,
                before.as_secs_f64() / after.as_secs_f64(),
            );
        }
    }

    fn counters(entries: usize) -> MetricsSet {
        let mut metrics = MetricsSet::new();
        for partition in 0..entries {
            let count = Count::new();
            count.add(partition + 1);
            metrics.push(Arc::new(Metric::new(
                MetricValue::OutputRows(count),
                Some(partition),
            )));
        }
        metrics
    }

    fn wrapper(local: MetricsSet, snapshot: MetricsSet) -> MetricsWrapperExec {
        let inner =
            MetricsWrapperExec::new(Arc::new(EmptyExec::new(Arc::new(Schema::empty()))), local);
        MetricsWrapperExec::new(Arc::new(inner), snapshot)
    }

    fn legacy_metrics(wrapper: &MetricsWrapperExec) -> MetricsSet {
        let mut metrics = wrapper.metrics.clone();
        for local in wrapper.inner.metrics().unwrap_or_default() {
            if !wrapper.metrics.iter().any(|wrapped| {
                wrapped.value() == local.value() && wrapped.partition() == local.partition()
            }) {
                metrics.push(local);
            }
        }
        metrics
    }
}
