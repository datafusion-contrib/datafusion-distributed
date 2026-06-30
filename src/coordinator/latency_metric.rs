use datafusion::common::instant::Instant;
use datafusion::physical_expr_common::metrics::{
    ExecutionPlanMetricsSet, MetricBuilder, MetricValue, Time,
};
use std::fmt::Display;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// DataFusion metrics system is pretty limited from an API standpoint. This intermediate struct
/// bridges the gaps that are not satisfied by upstream API for measuring latency.
pub struct LatencyMetric {
    max: Time,
    avg: Time,
    max_latency_micros: AtomicU64,
    sum_latency_micros: AtomicU64,
    count_latency_micros: AtomicU64,
}

impl Drop for LatencyMetric {
    fn drop(&mut self) {
        self.max.add_duration(Duration::from_micros(
            self.max_latency_micros.load(Ordering::Relaxed),
        ));
        self.avg.add_duration(Duration::from_micros(
            self.sum_latency_micros.load(Ordering::Relaxed)
                / self.count_latency_micros.load(Ordering::Relaxed).max(1),
        ));
    }
}

impl LatencyMetric {
    pub(super) fn new(
        name: impl Display,
        builder: impl Fn(MetricBuilder) -> MetricBuilder,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let max = Time::new();
        builder(MetricBuilder::new(metrics)).build(MetricValue::Time {
            name: format!("{name}_max").into(),
            time: max.clone(),
        });
        let avg = Time::new();
        builder(MetricBuilder::new(metrics)).build(MetricValue::Time {
            name: format!("{name}_avg").into(),
            time: avg.clone(),
        });
        Self {
            max,
            avg,
            max_latency_micros: AtomicU64::new(0),
            sum_latency_micros: AtomicU64::new(0),
            count_latency_micros: AtomicU64::new(0),
        }
    }

    pub fn record(&self, start: &Instant) {
        let micros = start.elapsed().as_micros() as u64;
        self.max_latency_micros.fetch_max(micros, Ordering::Relaxed);
        self.sum_latency_micros.fetch_add(micros, Ordering::Relaxed);
        self.count_latency_micros.fetch_add(1, Ordering::Relaxed);
    }
}
