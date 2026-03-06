use datafusion::common::human_readable_duration;
use datafusion::common::instant::Instant;
use datafusion::physical_expr_common::metrics::{MetricBuilder, MetricValue};
use datafusion::physical_plan::metrics::CustomMetricValue;
use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

/// Extension trait for DataFusion's metric system that adds support for latency related metrics.
pub trait LatencyMetricExt {
    fn min_latency(self, name: impl Into<Cow<'static, str>>) -> MinLatencyMetric;
    fn max_latency(self, name: impl Into<Cow<'static, str>>) -> MaxLatencyMetric;
    fn avg_latency(self, name: impl Into<Cow<'static, str>>) -> AvgLatencyMetric;
    fn first_latency(self, name: impl Into<Cow<'static, str>>) -> FirstLatencyMetric;
    fn sum_latency(self, name: impl Into<Cow<'static, str>>) -> SumLatencyMetric;
    fn count_latency(self, name: impl Into<Cow<'static, str>>) -> CountLatencyMetric;
}

impl LatencyMetricExt for MetricBuilder<'_> {
    fn min_latency(self, name: impl Into<Cow<'static, str>>) -> MinLatencyMetric {
        let value = MinLatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }

    fn max_latency(self, name: impl Into<Cow<'static, str>>) -> MaxLatencyMetric {
        let value = MaxLatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }

    fn avg_latency(self, name: impl Into<Cow<'static, str>>) -> AvgLatencyMetric {
        let value = AvgLatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }

    fn first_latency(self, name: impl Into<Cow<'static, str>>) -> FirstLatencyMetric {
        let value = FirstLatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }

    fn sum_latency(self, name: impl Into<Cow<'static, str>>) -> SumLatencyMetric {
        let value = SumLatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }

    fn count_latency(self, name: impl Into<Cow<'static, str>>) -> CountLatencyMetric {
        let value = CountLatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }
}

#[derive(Debug, Clone)]
pub struct MinLatencyMetric {
    nanos: Arc<AtomicUsize>,
}

impl Default for MinLatencyMetric {
    fn default() -> Self {
        Self {
            nanos: Arc::new(AtomicUsize::new(usize::MAX)),
        }
    }
}

impl MinLatencyMetric {
    pub fn from_nanos(nanos: usize) -> Self {
        Self {
            nanos: Arc::new(AtomicUsize::new(nanos)),
        }
    }

    pub fn value(&self) -> usize {
        self.nanos.load(Relaxed)
    }

    pub fn add_elapsed(&self, start: Instant) {
        self.add_duration(start.elapsed());
    }

    pub fn add_duration(&self, duration: Duration) {
        let more_nanos = duration.as_nanos() as usize;
        self.nanos.fetch_min(more_nanos.max(1), Relaxed);
    }
}

impl Display for MinLatencyMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value() {
            usize::MAX => write!(f, "0ns"),
            v => write!(f, "{}", human_readable_duration(v as u64)),
        }
    }
}

impl CustomMetricValue for MinLatencyMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(MinLatencyMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.nanos.fetch_min(other.nanos.load(Relaxed), Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.value()
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        other.value() == self.value()
    }
}

#[derive(Debug, Clone, Default)]
pub struct MaxLatencyMetric {
    nanos: Arc<AtomicUsize>,
}

impl MaxLatencyMetric {
    pub fn from_nanos(nanos: usize) -> Self {
        Self {
            nanos: Arc::new(AtomicUsize::new(nanos)),
        }
    }

    pub fn value(&self) -> usize {
        self.nanos.load(Relaxed)
    }

    pub fn add_elapsed(&self, start: Instant) {
        self.add_duration(start.elapsed());
    }

    pub fn add_duration(&self, duration: Duration) {
        let more_nanos = duration.as_nanos() as usize;
        self.nanos.fetch_max(more_nanos.max(1), Relaxed);
    }
}

impl Display for MaxLatencyMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", human_readable_duration(self.value() as u64))
    }
}

impl CustomMetricValue for MaxLatencyMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(MaxLatencyMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.nanos.fetch_max(other.nanos.load(Relaxed), Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.value()
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        other.value() == self.value()
    }
}

#[derive(Debug, Clone, Default)]
pub struct AvgLatencyMetric {
    nanos_sum: Arc<AtomicUsize>,
    count: Arc<AtomicUsize>,
}

impl AvgLatencyMetric {
    pub(crate) fn from_raw(nanos_sum: usize, count: usize) -> Self {
        Self {
            nanos_sum: Arc::new(AtomicUsize::new(nanos_sum)),
            count: Arc::new(AtomicUsize::new(count)),
        }
    }

    pub fn value(&self) -> usize {
        self.nanos_sum.load(Relaxed) / self.count.load(Relaxed).max(1)
    }

    pub(crate) fn nanos_sum(&self) -> usize {
        self.nanos_sum.load(Relaxed)
    }

    pub(crate) fn count(&self) -> usize {
        self.count.load(Relaxed)
    }

    pub fn add_elapsed(&self, start: Instant) {
        self.add_duration(start.elapsed());
    }

    pub fn add_duration(&self, duration: Duration) {
        let more_nanos = duration.as_nanos() as usize;
        self.nanos_sum.fetch_add(more_nanos.max(1), Relaxed);
        self.count.fetch_add(1, Relaxed);
    }
}

impl Display for AvgLatencyMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", human_readable_duration(self.value() as u64))
    }
}

impl CustomMetricValue for AvgLatencyMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(AvgLatencyMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.nanos_sum
            .fetch_add(other.nanos_sum.load(Relaxed), Relaxed);
        self.count.fetch_add(other.count.load(Relaxed), Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.value()
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        other.value() == self.value()
    }
}

/// A latency metric that captures only the first recorded value, ignoring all subsequent ones.
/// Uses 0 as the unset sentinel (valid durations are clamped to at least 1 nanosecond).
#[derive(Debug, Clone, Default)]
pub struct FirstLatencyMetric {
    nanos: Arc<AtomicUsize>,
}

impl FirstLatencyMetric {
    pub fn from_nanos(nanos: usize) -> Self {
        Self {
            nanos: Arc::new(AtomicUsize::new(nanos)),
        }
    }

    pub fn value(&self) -> usize {
        self.nanos.load(Relaxed)
    }

    pub fn add_elapsed(&self, start: Instant) {
        self.add_duration(start.elapsed());
    }

    pub fn add_duration(&self, duration: Duration) {
        let nanos = duration.as_nanos() as usize;
        // compare_exchange: only set if still at the sentinel value (0).
        let _ = self
            .nanos
            .compare_exchange(0, nanos.max(1), Relaxed, Relaxed);
    }
}

impl Display for FirstLatencyMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", human_readable_duration(self.value() as u64))
    }
}

impl CustomMetricValue for FirstLatencyMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(FirstLatencyMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        // Keep self's value if already set, otherwise take other's.
        let _ = self
            .nanos
            .compare_exchange(0, other.nanos.load(Relaxed), Relaxed, Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.value()
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        other.value() == self.value()
    }
}
#[derive(Debug, Clone, Default)]
pub struct SumLatencyMetric {
    nanos: Arc<AtomicUsize>,
}

impl SumLatencyMetric {
    pub fn from_nanos(nanos: usize) -> Self {
        Self {
            nanos: Arc::new(AtomicUsize::new(nanos)),
        }
    }

    pub fn value(&self) -> usize {
        self.nanos.load(Relaxed)
    }

    pub fn add_elapsed(&self, start: Instant) {
        self.add_duration(start.elapsed());
    }

    pub fn add_duration(&self, duration: Duration) {
        let more_nanos = duration.as_nanos() as usize;
        self.nanos.fetch_add(more_nanos.max(1), Relaxed);
    }
}

impl Display for SumLatencyMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", human_readable_duration(self.value() as u64))
    }
}

impl CustomMetricValue for SumLatencyMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(SumLatencyMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.nanos.fetch_add(other.nanos.load(Relaxed), Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.value()
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        other.value() == self.value()
    }
}

#[derive(Debug, Clone, Default)]
pub struct CountLatencyMetric {
    count: Arc<AtomicUsize>,
}

impl CountLatencyMetric {
    pub fn from_count(count: usize) -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(count)),
        }
    }

    pub fn value(&self) -> usize {
        self.count.load(Relaxed)
    }

    pub fn add(&self, n: usize) {
        self.count.fetch_add(n, Relaxed);
    }
}

impl Display for CountLatencyMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

impl CustomMetricValue for CountLatencyMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(CountLatencyMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.count.fetch_add(other.count.load(Relaxed), Relaxed);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_usize(&self) -> usize {
        self.value()
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return false;
        };
        other.value() == self.value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn min_latency_tracks_minimum_and_aggregates() {
        let m = MinLatencyMetric::default();
        assert_eq!(m.value(), usize::MAX);
        m.add_duration(Duration::from_millis(100));
        m.add_duration(Duration::from_millis(50));
        m.add_duration(Duration::from_millis(200));
        assert_eq!(m.value(), Duration::from_millis(50).as_nanos() as usize);

        let other = MinLatencyMetric::default();
        other.add_duration(Duration::from_millis(10));
        m.aggregate(Arc::new(other));
        assert_eq!(m.value(), Duration::from_millis(10).as_nanos() as usize);
    }

    #[test]
    fn max_latency_tracks_maximum_and_aggregates() {
        let m = MaxLatencyMetric::default();
        assert_eq!(m.value(), 0);
        m.add_duration(Duration::from_millis(100));
        m.add_duration(Duration::from_millis(200));
        m.add_duration(Duration::from_millis(50));
        assert_eq!(m.value(), Duration::from_millis(200).as_nanos() as usize);

        let other = MaxLatencyMetric::default();
        other.add_duration(Duration::from_millis(500));
        m.aggregate(Arc::new(other));
        assert_eq!(m.value(), Duration::from_millis(500).as_nanos() as usize);
    }

    #[test]
    fn avg_latency_computes_average_and_aggregates() {
        let m = AvgLatencyMetric::default();
        assert_eq!(m.value(), 0);
        m.add_duration(Duration::from_millis(100));
        m.add_duration(Duration::from_millis(200));
        assert_eq!(m.value(), Duration::from_millis(150).as_nanos() as usize);

        let other = AvgLatencyMetric::default();
        other.add_duration(Duration::from_millis(300));
        m.aggregate(Arc::new(other));
        // sum=600ms, count=3 -> avg=200ms
        assert_eq!(m.value(), Duration::from_millis(200).as_nanos() as usize);
    }

    #[test]
    fn first_latency_captures_first_value_and_aggregates() {
        let m = FirstLatencyMetric::default();
        assert_eq!(m.value(), 0);
        m.add_duration(Duration::from_millis(100));
        m.add_duration(Duration::from_millis(200));
        assert_eq!(m.value(), Duration::from_millis(100).as_nanos() as usize);

        // Aggregate keeps self's value when already set.
        let other = FirstLatencyMetric::default();
        other.add_duration(Duration::from_millis(50));
        m.aggregate(Arc::new(other));
        assert_eq!(m.value(), Duration::from_millis(100).as_nanos() as usize);

        // Aggregate takes other's value when self is unset.
        let unset = FirstLatencyMetric::default();
        let other2 = FirstLatencyMetric::default();
        other2.add_duration(Duration::from_millis(77));
        unset.aggregate(Arc::new(other2));
        assert_eq!(unset.value(), Duration::from_millis(77).as_nanos() as usize);
    }

    #[test]
    fn sum_latency_sums_all_values_and_aggregates() {
        let m = SumLatencyMetric::default();
        assert_eq!(m.value(), 0);
        m.add_duration(Duration::from_millis(100));
        m.add_duration(Duration::from_millis(200));
        m.add_duration(Duration::from_millis(50));
        assert_eq!(m.value(), Duration::from_millis(350).as_nanos() as usize);

        let other = SumLatencyMetric::default();
        other.add_duration(Duration::from_millis(150));
        m.aggregate(Arc::new(other));
        assert_eq!(m.value(), Duration::from_millis(500).as_nanos() as usize);
    }

    #[test]
    fn count_latency_increments_and_aggregates() {
        let m = CountLatencyMetric::default();
        assert_eq!(m.value(), 0);
        m.add(1);
        m.add(1);
        m.add(3);
        assert_eq!(m.value(), 5);

        let other = CountLatencyMetric::default();
        other.add(7);
        m.aggregate(Arc::new(other));
        assert_eq!(m.value(), 12);
    }

    #[test]
    fn zero_duration_clamped_to_one_nano() {
        let min = MinLatencyMetric::default();
        min.add_duration(Duration::ZERO);
        assert_eq!(min.value(), 1);

        let max = MaxLatencyMetric::default();
        max.add_duration(Duration::ZERO);
        assert_eq!(max.value(), 1);

        let avg = AvgLatencyMetric::default();
        avg.add_duration(Duration::ZERO);
        assert_eq!(avg.value(), 1);

        let first = FirstLatencyMetric::default();
        first.add_duration(Duration::ZERO);
        assert_eq!(first.value(), 1);

        let sum = SumLatencyMetric::default();
        sum.add_duration(Duration::ZERO);
        assert_eq!(sum.value(), 1);
    }
}
