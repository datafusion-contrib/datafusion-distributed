use datafusion::physical_plan::metrics::{CustomMetricValue, MetricBuilder, MetricValue};
use std::sync::atomic::Ordering::Relaxed;
use std::{
    any::Any,
    borrow::Cow,
    fmt::{Display, Formatter},
    sync::{Arc, atomic::AtomicUsize},
};

/// Extension trait for DataFusion's metric system that adds support for a Gauge metric that
/// aggregates to others using `max` instead of `sum`
pub trait GaugeMetricExt {
    fn max_gauge(self, name: impl Into<Cow<'static, str>>) -> MaxGaugeMetric;
}

impl GaugeMetricExt for MetricBuilder<'_> {
    fn max_gauge(self, name: impl Into<Cow<'static, str>>) -> MaxGaugeMetric {
        let value = MaxGaugeMetric::default();
        self.build(MetricValue::Custom {
            name: name.into(),
            value: Arc::new(value.clone()),
        });
        value
    }
}

/// Similar to DataFusion's Gauge metric, but aggregates between instances using `max` instead of
/// `sum`.
#[derive(Debug, Clone)]
pub struct MaxGaugeMetric {
    value: Arc<AtomicUsize>,
}

impl Default for MaxGaugeMetric {
    fn default() -> Self {
        Self {
            value: Arc::new(AtomicUsize::new(usize::MIN)),
        }
    }
}

impl MaxGaugeMetric {
    pub fn from_value(bytes: usize) -> Self {
        Self {
            value: Arc::new(AtomicUsize::new(bytes)),
        }
    }

    pub fn value(&self) -> usize {
        self.value.load(Relaxed)
    }

    pub fn set_max(&self, n: usize) {
        self.value.fetch_max(n, Relaxed);
    }
}

impl Display for MaxGaugeMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value())
    }
}

impl CustomMetricValue for MaxGaugeMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(MaxGaugeMetric::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.value.fetch_max(other.value.load(Relaxed), Relaxed);
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
    fn default_is_zero_and_set_max_updates() {
        let m = MaxGaugeMetric::default();
        assert_eq!(m.value(), 0);
        m.set_max(1024);
        assert_eq!(m.value(), 1024);
        // Lower value should not decrease the gauge
        m.set_max(512);
        assert_eq!(m.value(), 1024);
        // Higher value should increase it
        m.set_max(2048);
        assert_eq!(m.value(), 2048);
    }

    #[test]
    fn from_value_constructs_correctly() {
        let m = MaxGaugeMetric::from_value(1_000_000);
        assert_eq!(m.value(), 1_000_000);
    }

    #[test]
    fn aggregate_takes_max() {
        let a = MaxGaugeMetric::from_value(500);
        let b = MaxGaugeMetric::from_value(300);
        a.aggregate(Arc::new(b));
        assert_eq!(a.value(), 500);

        let a = MaxGaugeMetric::from_value(300);
        let b = MaxGaugeMetric::from_value(500);
        a.aggregate(Arc::new(b));
        assert_eq!(a.value(), 500);
    }
}
