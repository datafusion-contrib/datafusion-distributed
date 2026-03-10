use std::{
    any::Any,
    borrow::Cow,
    fmt::{Display, Formatter},
    sync::{Arc, atomic::AtomicUsize},
};

use datafusion::{
    common::human_readable_size,
    physical_plan::metrics::{CustomMetricValue, MetricBuilder, MetricValue},
};
use std::sync::atomic::Ordering::Relaxed;

/// Extension trait for DataFusion's metric system that adds support for byte count metrics
/// that display using human-readable byte sizes (KB, MB, GB) instead of plain count notation.
pub trait BytesMetricExt {
    fn bytes_counter(self, name: impl Into<Cow<'static, str>>) -> BytesCounterMetric;
}

impl BytesMetricExt for MetricBuilder<'_> {
    fn bytes_counter(self, name: impl Into<Cow<'static, str>>) -> BytesCounterMetric {
        let value = BytesCounterMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }
}
/// A cumulative counter metric for tracking byte counts.
///
/// Unlike DataFusion's built-in [`Count`](datafusion::physical_plan::metrics::Count) which formats
/// large values using plain count notation (e.g., "1.91 B" meaning 1.91 billion), this metric
/// formats values using [`human_readable_size`] (e.g., "1.91 GB").
///
/// This avoids the confusing display where "B" (billions) looks like "bytes".
///
/// Aggregation sums values across partitions/tasks.
#[derive(Debug, Clone)]
pub struct BytesCounterMetric {
    bytes: Arc<AtomicUsize>,
}

impl Default for BytesCounterMetric {
    fn default() -> Self {
        Self {
            bytes: Arc::new(AtomicUsize::new(usize::MIN)),
        }
    }
}

impl BytesCounterMetric {
    pub fn from_value(bytes: usize) -> Self {
        Self {
            bytes: Arc::new(AtomicUsize::new(bytes)),
        }
    }

    pub fn value(&self) -> usize {
        self.bytes.load(Relaxed)
    }

    pub fn add_bytes(&self, bytes: usize) -> usize {
        self.bytes.fetch_add(bytes, Relaxed)
    }
}

impl Display for BytesCounterMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", human_readable_size(self.value()))
    }
}

impl CustomMetricValue for BytesCounterMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(BytesCounterMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.bytes.fetch_add(other.bytes.load(Relaxed), Relaxed);
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
    fn default_is_zero_and_add_accumulates() {
        let m = BytesCounterMetric::default();
        assert_eq!(m.value(), 0);
        m.add_bytes(1024);
        m.add_bytes(2048);
        assert_eq!(m.value(), 3072);
    }

    #[test]
    fn from_value_constructs_correctly() {
        let m = BytesCounterMetric::from_value(1_000_000);
        assert_eq!(m.value(), 1_000_000);
    }

    #[test]
    fn aggregate_sums_values() {
        let a = BytesCounterMetric::from_value(500);
        let b = BytesCounterMetric::from_value(300);
        a.aggregate(Arc::new(b));
        assert_eq!(a.value(), 800);
    }

    #[test]
    fn display_uses_human_readable_size() {
        // 0 bytes
        assert_eq!(format!("{}", BytesCounterMetric::from_value(0)), "0.0 B");
        // 4 MB (>= 2*MB threshold, so displays in MB)
        assert_eq!(
            format!("{}", BytesCounterMetric::from_value(4 * 1024 * 1024)),
            "4.0 MB"
        );
        // 4 GB (>= 2*GB threshold, so displays in GB)
        assert_eq!(
            format!("{}", BytesCounterMetric::from_value(4 * 1024 * 1024 * 1024)),
            "4.0 GB"
        );
    }
}
