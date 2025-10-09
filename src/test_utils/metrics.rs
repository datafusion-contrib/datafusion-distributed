use crate::metrics::proto::{ElapsedCompute, EndTimestamp, OutputRows, StartTimestamp};
use crate::metrics::proto::{MetricProto, MetricValueProto, MetricsSetProto};

/// creates a "distinct" set of metrics from the provided seed
pub fn make_test_metrics_set_proto_from_seed(seed: u64, num_metrics: usize) -> MetricsSetProto {
    const TEST_TIMESTAMP: i64 = 1758200400000000000; // 2025-09-18 13:00:00 UTC

    let mut result = MetricsSetProto { metrics: vec![] };

    for i in 0..num_metrics {
        let value = seed + i as u64;
        result.push(match i % 4 {
            0 => MetricProto {
                metric: Some(MetricValueProto::OutputRows(OutputRows { value })),
                labels: vec![],
                partition: None,
            },

            1 => MetricProto {
                metric: Some(MetricValueProto::ElapsedCompute(ElapsedCompute { value })),
                labels: vec![],
                partition: None,
            },
            2 => MetricProto {
                metric: Some(MetricValueProto::StartTimestamp(StartTimestamp {
                    value: Some(TEST_TIMESTAMP + (value as i64 * 1_000_000_000)),
                })),
                labels: vec![],
                partition: None,
            },
            3 => MetricProto {
                metric: Some(MetricValueProto::EndTimestamp(EndTimestamp {
                    value: Some(TEST_TIMESTAMP + (value as i64 * 1_000_000_000)),
                })),
                labels: vec![],
                partition: None,
            },
            _ => unreachable!(),
        })
    }
    result
}
