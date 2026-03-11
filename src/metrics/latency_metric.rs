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
    fn p50_latency(self, name: impl Into<Cow<'static, str>>) -> P50LatencyMetric;
    fn p75_latency(self, name: impl Into<Cow<'static, str>>) -> P75LatencyMetric;
    fn p95_latency(self, name: impl Into<Cow<'static, str>>) -> P95LatencyMetric;
    fn p99_latency(self, name: impl Into<Cow<'static, str>>) -> P99LatencyMetric;
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

    fn p50_latency(self, name: impl Into<Cow<'static, str>>) -> P50LatencyMetric {
        let value = P50LatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }

    fn p75_latency(self, name: impl Into<Cow<'static, str>>) -> P75LatencyMetric {
        let value = P75LatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }

    fn p95_latency(self, name: impl Into<Cow<'static, str>>) -> P95LatencyMetric {
        let value = P95LatencyMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }

    fn p99_latency(self, name: impl Into<Cow<'static, str>>) -> P99LatencyMetric {
        let value = P99LatencyMetric::default();
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

const NUM_BUCKETS: usize = 64;

/// Shared histogram storage for percentile latency metrics.
/// Uses power-of-2 logarithmic buckets: bucket i covers [2^i, 2^(i+1)) nanoseconds.
#[derive(Debug)]
struct LatencyHistogram {
    buckets: [AtomicUsize; NUM_BUCKETS],
    total: AtomicUsize,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicUsize::new(0)),
            total: AtomicUsize::new(0),
        }
    }
}

impl LatencyHistogram {
    fn from_raw(buckets: &[u64], total: usize) -> Self {
        let histogram = Self::default();
        for (i, &count) in buckets.iter().enumerate().take(NUM_BUCKETS) {
            histogram.buckets[i].store(count as usize, Relaxed);
        }
        histogram.total.store(total, Relaxed);
        histogram
    }

    fn bucket_counts(&self) -> Vec<u64> {
        self.buckets
            .iter()
            .map(|b| b.load(Relaxed) as u64)
            .collect()
    }

    fn total(&self) -> usize {
        self.total.load(Relaxed)
    }

    fn record(&self, nanos: usize) {
        let nanos = nanos.max(1);
        let idx = (usize::BITS - 1 - nanos.leading_zeros()) as usize;
        self.buckets[idx].fetch_add(1, Relaxed);
        self.total.fetch_add(1, Relaxed);
    }

    fn percentile_value(&self, percentile: f64) -> usize {
        let total = self.total.load(Relaxed);
        if total == 0 {
            return 0;
        }
        let threshold = ((total as f64) * percentile).ceil() as usize;
        let mut cumulative = 0;
        for (i, bucket) in self.buckets.iter().enumerate() {
            cumulative += bucket.load(Relaxed);
            if cumulative >= threshold {
                // Return the midpoint of the bucket: (2^i + 2^(i+1)) / 2 = 3 * 2^(i-1)
                return if i == 0 { 1 } else { 3 * (1usize << (i - 1)) };
            }
        }
        // Should not reach here, but return the max bucket midpoint
        3 * (1usize << (NUM_BUCKETS - 2))
    }

    fn aggregate_from(&self, other: &LatencyHistogram) {
        for (i, bucket) in self.buckets.iter().enumerate() {
            bucket.fetch_add(other.buckets[i].load(Relaxed), Relaxed);
        }
        self.total.fetch_add(other.total.load(Relaxed), Relaxed);
    }
}

macro_rules! percentile_latency_metric {
    ($name:ident, $percentile:expr) => {
        #[derive(Debug, Clone)]
        pub struct $name {
            inner: Arc<LatencyHistogram>,
        }

        impl Default for $name {
            fn default() -> Self {
                Self {
                    inner: Arc::new(LatencyHistogram::default()),
                }
            }
        }

        impl $name {
            pub(crate) fn from_raw(buckets: &[u64], total: usize) -> Self {
                Self {
                    inner: Arc::new(LatencyHistogram::from_raw(buckets, total)),
                }
            }

            pub fn value(&self) -> usize {
                self.inner.percentile_value($percentile)
            }

            pub(crate) fn bucket_counts(&self) -> Vec<u64> {
                self.inner.bucket_counts()
            }

            pub(crate) fn total(&self) -> usize {
                self.inner.total()
            }

            pub fn add_elapsed(&self, start: Instant) {
                self.add_duration(start.elapsed());
            }

            pub fn add_duration(&self, duration: Duration) {
                self.inner.record(duration.as_nanos() as usize);
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", human_readable_duration(self.value() as u64))
            }
        }

        impl CustomMetricValue for $name {
            fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
                Arc::new($name::default())
            }

            fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
                let Some(other) = other.as_any().downcast_ref::<Self>() else {
                    return;
                };
                self.inner.aggregate_from(&other.inner);
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
    };
}

percentile_latency_metric!(P50LatencyMetric, 0.50);
percentile_latency_metric!(P75LatencyMetric, 0.75);
percentile_latency_metric!(P95LatencyMetric, 0.95);
percentile_latency_metric!(P99LatencyMetric, 0.99);

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
    fn p50_latency_returns_median_bucket() {
        let m = P50LatencyMetric::default();
        assert_eq!(m.value(), 0);
        // Add 100 samples: 50 at 1ms, 50 at 100ms
        for _ in 0..50 {
            m.add_duration(Duration::from_millis(1));
        }
        for _ in 0..50 {
            m.add_duration(Duration::from_millis(100));
        }
        // p50 should fall in the 1ms bucket range
        let val = m.value();
        assert!(val < Duration::from_millis(2).as_nanos() as usize);
    }

    #[test]
    fn p99_latency_returns_high_bucket() {
        let m = P99LatencyMetric::default();
        // Add 98 samples at 1ms and 2 samples at 100ms
        for _ in 0..98 {
            m.add_duration(Duration::from_millis(1));
        }
        m.add_duration(Duration::from_millis(100));
        m.add_duration(Duration::from_millis(100));
        // p99 = ceil(100 * 0.99) = 99th sample, which falls in the 100ms bucket
        let val = m.value();
        assert!(val >= Duration::from_millis(50).as_nanos() as usize);
    }

    #[test]
    fn percentile_latency_aggregates() {
        let m1 = P75LatencyMetric::default();
        let m2 = P75LatencyMetric::default();
        for _ in 0..75 {
            m1.add_duration(Duration::from_millis(1));
        }
        for _ in 0..25 {
            m2.add_duration(Duration::from_millis(100));
        }
        m1.aggregate(Arc::new(m2));
        // After aggregation: 75 at 1ms, 25 at 100ms. p75 should be in the 1ms range.
        let val = m1.value();
        assert!(val < Duration::from_millis(2).as_nanos() as usize);
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
    }
}
