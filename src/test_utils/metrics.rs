use crate::coordinator::DistributedExec;
use chrono::{DateTime, Utc};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{Count, Metric, MetricValue, MetricsSet, Time, Timestamp};
use std::sync::Arc;

/// Waits until all worker tasks have reported their metrics back via the coordinator channel.
pub async fn wait_for_all_metrics(plan: &Arc<dyn ExecutionPlan>) {
    if let Some(dist_exec) = plan.downcast_ref::<DistributedExec>() {
        dist_exec.wait_for_metrics().await;
    }
}

/// creates a "distinct" set of metrics from the provided seed
pub fn make_test_metrics_set_from_seed(seed: u64, num_metrics: usize) -> MetricsSet {
    const TEST_TIMESTAMP: i64 = 1758200400000000000; // 2025-09-18 13:00:00 UTC

    let mut result = MetricsSet::new();

    for i in 0..num_metrics {
        let value = (seed + i as u64) as usize;
        result.push(Arc::new(Metric::new(
            match i % 4 {
                0 => {
                    let count = Count::new();
                    count.add(value);
                    MetricValue::OutputRows(count)
                }
                1 => {
                    let time = Time::new();
                    time.add_duration(std::time::Duration::from_nanos(value as u64));
                    MetricValue::ElapsedCompute(time)
                }
                2 => MetricValue::StartTimestamp(timestamp_from_nanos(
                    TEST_TIMESTAMP + (value as i64 * 1_000_000_000),
                )),
                3 => MetricValue::EndTimestamp(timestamp_from_nanos(
                    TEST_TIMESTAMP + (value as i64 * 1_000_000_000),
                )),
                _ => unreachable!(),
            },
            None,
        )))
    }
    result
}

fn timestamp_from_nanos(nanos: i64) -> Timestamp {
    let timestamp = Timestamp::new();
    timestamp.set(DateTime::<Utc>::from_timestamp_nanos(nanos));
    timestamp
}
