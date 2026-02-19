use sketches_ddsketch::{Config, DDSketch};
use std::time::Duration;

/// Tracks latency stats using running aggregates and a DDSketch for quantiles.
#[derive(Clone)]
pub struct LatencyTracker {
    // first sample
    first_nanos: Option<u64>,
    // DDSketch provides relative-error quantiles with bounded size.
    sketch: DDSketch,
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyTracker {
    /// Creates a new empty tracker.
    pub fn new() -> Self {
        Self {
            first_nanos: None,
            sketch: DDSketch::new(Config::defaults()),
        }
    }

    /// Adds a latency sample to the tracker.
    pub fn track(&mut self, duration: Duration) {
        let nanos = duration.as_nanos() as u64;

        if self.sketch.count() == 0 {
            self.first_nanos = Some(nanos);
        }

        self.sketch.add(nanos as f64);
    }

    /// Returns the number of samples recorded.
    pub fn count(&self) -> u64 {
        self.sketch.count() as u64
    }

    /// Returns the first recorded latency, if any.
    pub fn first(&self) -> Option<Duration> {
        self.first_nanos.map(Duration::from_nanos)
    }

    /// Returns the minimum latency seen, if any.
    pub fn min(&self) -> Option<Duration> {
        let value = self.sketch.min()?;
        if value.is_finite() && value >= 0.0 {
            Some(Duration::from_nanos(value.round() as u64))
        } else {
            None
        }
    }

    /// Returns the maximum latency seen, if any.
    pub fn max(&self) -> Option<Duration> {
        let value = self.sketch.max()?;
        if value.is_finite() && value >= 0.0 {
            Some(Duration::from_nanos(value.round() as u64))
        } else {
            None
        }
    }

    /// Returns the average latency, if any.
    pub fn avg(&self) -> Option<Duration> {
        let sum = self.sketch.sum()?;
        let count = self.count();
        if count == 0 {
            return None;
        }
        let avg = sum / count as f64;
        if avg.is_finite() && avg >= 0.0 {
            Some(Duration::from_nanos(avg.round() as u64))
        } else {
            None
        }
    }

    /// Returns the estimated latency at the given quantile (0.0..=1.0).
    pub fn quantile(&self, q: f64) -> Option<Duration> {
        if self.count() == 0 {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        let value = self.sketch.quantile(q).ok().flatten()?;
        if value.is_finite() && value >= 0.0 {
            Some(Duration::from_nanos(value.round() as u64))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tracker_returns_none() {
        let tracker = LatencyTracker::new();
        assert_eq!(tracker.count(), 0);
        assert!(tracker.first().is_none());
        assert!(tracker.min().is_none());
        assert!(tracker.max().is_none());
        assert!(tracker.avg().is_none());
        assert!(tracker.quantile(0.90).is_none());
        assert!(tracker.quantile(0.99).is_none());
    }

    #[test]
    fn basic_stats_and_quantiles() {
        let mut tracker = LatencyTracker::new();
        for nanos in [10u64, 20, 30, 40, 50] {
            tracker.track(Duration::from_nanos(nanos));
        }

        assert_eq!(tracker.count(), 5);
        assert_eq!(tracker.first(), Some(Duration::from_nanos(10)));
        assert_eq!(tracker.min(), Some(Duration::from_nanos(10)));
        assert_eq!(tracker.max(), Some(Duration::from_nanos(50)));
        assert_eq!(tracker.avg(), Some(Duration::from_nanos(30)));
        let p90 = tracker.quantile(0.90).unwrap().as_nanos() as i64;
        let p99 = tracker.quantile(0.99).unwrap().as_nanos() as i64;
        // DDSketch provides approximate quantiles with bounded relative error,
        // so we use a wider range to account for this.
        assert!((40..=60).contains(&p90));
        assert!((40..=60).contains(&p99));
    }
}
