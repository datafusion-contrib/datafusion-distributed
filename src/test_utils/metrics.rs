use crate::metrics::proto::{ElapsedCompute, EndTimestamp, OutputRows, StartTimestamp};
use crate::metrics::proto::{MetricProto, MetricValueProto, MetricsSetProto};

/// creates a "distinct" set of metrics from the provided seed
pub fn make_test_metrics_set_proto_from_seed(seed: u64) -> MetricsSetProto {
    const TEST_TIMESTAMP: i64 = 1758200400000000000; // 2025-09-18 13:00:00 UTC
    MetricsSetProto {
        metrics: vec![
            MetricProto {
                metric: Some(MetricValueProto::OutputRows(OutputRows { value: seed })),
                labels: vec![],
                partition: None,
            },
            MetricProto {
                metric: Some(MetricValueProto::ElapsedCompute(ElapsedCompute {
                    value: seed,
                })),
                labels: vec![],
                partition: None,
            },
            MetricProto {
                metric: Some(MetricValueProto::StartTimestamp(StartTimestamp {
                    value: Some(TEST_TIMESTAMP + (seed as i64 * 1_000_000_000)),
                })),
                labels: vec![],
                partition: None,
            },
            MetricProto {
                metric: Some(MetricValueProto::EndTimestamp(EndTimestamp {
                    value: Some(TEST_TIMESTAMP + ((seed as i64 + 1) * 1_000_000_000)),
                })),
                labels: vec![],
                partition: None,
            },
        ],
    }
}
