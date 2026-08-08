use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::{CustomMetricValue, MetricValue};
use std::any::Any;
use std::borrow::Cow;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// A floating-point q-error metric.
///
/// When multiple values are aggregated, the largest q-error is retained so a bad estimate is not
/// hidden. A q-error of `1.0` represents a perfect estimate and is therefore the neutral value.
#[derive(Clone)]
pub struct QErrorMetric {
    value: Arc<AtomicU64>,
}

impl Default for QErrorMetric {
    fn default() -> Self {
        Self::from_value(1.0)
    }
}

impl QErrorMetric {
    pub fn new_metric(name: impl Into<Cow<'static, str>>, value: f64) -> Arc<Metric> {
        Arc::new(Metric::new(
            MetricValue::Custom {
                name: name.into(),
                value: Arc::new(Self::from_value(value)),
            },
            None,
        ))
    }

    pub fn from_value(value: f64) -> Self {
        Self {
            value: Arc::new(AtomicU64::new(value.to_bits())),
        }
    }

    pub fn value(&self) -> f64 {
        f64::from_bits(self.value.load(Relaxed))
    }
}

impl Debug for QErrorMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("QErrorMetric").field(&self.value()).finish()
    }
}

impl Display for QErrorMetric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = format!("{:.2}", self.value());
        write!(f, "{}x", value.trim_end_matches('0').trim_end_matches('.'))
    }
}

impl CustomMetricValue for QErrorMetric {
    fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
        Arc::new(Self::default())
    }

    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>) {
        let Some(other) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        let other = other.value();
        let _ = self.value.fetch_update(Relaxed, Relaxed, |value| {
            Some(f64::from_bits(value).max(other).to_bits())
        });
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
        other
            .as_any()
            .downcast_ref::<Self>()
            .is_some_and(|other| other.value().to_bits() == self.value().to_bits())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_a_perfect_estimate() {
        assert_eq!(QErrorMetric::default().value(), 1.0);
    }

    #[test]
    fn display_formats_as_a_factor() {
        assert_eq!(QErrorMetric::from_value(1.456).to_string(), "1.46x");
        assert_eq!(QErrorMetric::from_value(1.5).to_string(), "1.5x");
        assert_eq!(QErrorMetric::from_value(1.0).to_string(), "1x");
    }

    #[test]
    fn aggregate_retains_the_largest_q_error() {
        let metric = QErrorMetric::from_value(2.0);
        metric.aggregate(Arc::new(QErrorMetric::from_value(1.5)));
        metric.aggregate(Arc::new(QErrorMetric::from_value(4.25)));
        assert_eq!(metric.value(), 4.25);
    }
}
