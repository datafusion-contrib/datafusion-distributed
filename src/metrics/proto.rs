use chrono::DateTime;
use datafusion::common::internal_err;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::{Count, Gauge, Label, Time, Timestamp};
use datafusion::physical_plan::metrics::{Metric, MetricValue, MetricsSet};
use std::borrow::Cow;
use std::sync::Arc;

/// A MetricProto is a protobuf mirror of [datafusion::physical_plan::metrics::Metric].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricProto {
    #[prost(oneof = "MetricValueProto", tags = "1,2,3,4,5,6,7,8,9,10,11")]
    // This field is *always* set. It is marked optional due to protobuf "oneof" requirements.
    pub metric: Option<MetricValueProto>,
    #[prost(message, repeated, tag = "12")]
    pub labels: Vec<ProtoLabel>,
    #[prost(uint64, optional, tag = "13")]
    pub partition: Option<u64>,
}

/// A MetricsSetProto is a protobuf mirror of [datafusion::physical_plan::metrics::MetricsSet]. It represents
/// a collection of metrics for one `ExecutionPlan` node.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricsSetProto {
    #[prost(message, repeated, tag = "1")]
    pub metrics: Vec<MetricProto>,
}

/// MetricValueProto is a protobuf mirror of the [datafusion::physical_plan::metrics::MetricValue] enum.
#[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
pub enum MetricValueProto {
    #[prost(message, tag = "1")]
    OutputRows(OutputRows),
    #[prost(message, tag = "2")]
    ElapsedCompute(ElapsedCompute),
    #[prost(message, tag = "3")]
    SpillCount(SpillCount),
    #[prost(message, tag = "4")]
    SpilledBytes(SpilledBytes),
    #[prost(message, tag = "5")]
    SpilledRows(SpilledRows),
    #[prost(message, tag = "6")]
    CurrentMemoryUsage(CurrentMemoryUsage),
    #[prost(message, tag = "7")]
    Count(NamedCount),
    #[prost(message, tag = "8")]
    Gauge(NamedGauge),
    #[prost(message, tag = "9")]
    Time(NamedTime),
    #[prost(message, tag = "10")]
    StartTimestamp(StartTimestamp),
    #[prost(message, tag = "11")]
    EndTimestamp(EndTimestamp),
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct OutputRows {
    #[prost(uint64, tag = "1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ElapsedCompute {
    #[prost(uint64, tag = "1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct SpillCount {
    #[prost(uint64, tag = "1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct SpilledBytes {
    #[prost(uint64, tag = "1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct SpilledRows {
    #[prost(uint64, tag = "1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CurrentMemoryUsage {
    #[prost(uint64, tag = "1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct NamedCount {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(uint64, tag = "2")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct NamedGauge {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(uint64, tag = "2")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct NamedTime {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(uint64, tag = "2")]
    pub value: u64,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct StartTimestamp {
    #[prost(int64, optional, tag = "1")]
    pub value: Option<i64>,
}

#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct EndTimestamp {
    #[prost(int64, optional, tag = "1")]
    pub value: Option<i64>,
}

/// A ProtoLabel mirrors [datafusion::physical_plan::metrics::Label].
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct ProtoLabel {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// df_metrics_set_to_proto converts a [datafusion::physical_plan::metrics::MetricsSet] to a [MetricsSetProto].
/// Custom metrics are filtered out, but any other errors are returned.
/// TODO(#140): Support custom metrics.
#[allow(dead_code)]
pub fn df_metrics_set_to_proto(
    metrics_set: &MetricsSet,
) -> Result<MetricsSetProto, DataFusionError> {
    let mut metrics = Vec::new();

    for metric in metrics_set.iter() {
        match df_metric_to_proto(metric.clone()) {
            Ok(metric_proto) => metrics.push(metric_proto),
            Err(err) => {
                // Check if this is the specific custom metrics error we want to filter out
                if let DataFusionError::Internal(msg) = &err {
                    if msg == CUSTOM_METRICS_NOT_SUPPORTED {
                        // Filter out custom metrics error - continue processing other metrics
                        continue;
                    }
                }
                // Any other error should be returned
                return Err(err);
            }
        }
    }

    Ok(MetricsSetProto { metrics })
}

/// metrics_set_proto_to_df converts a [MetricsSetProto] to a [datafusion::physical_plan::metrics::MetricsSet].
#[allow(dead_code)]
pub fn metrics_set_proto_to_df(
    metrics_set_proto: &MetricsSetProto,
) -> Result<MetricsSet, DataFusionError> {
    let mut metrics_set = MetricsSet::new();
    metrics_set_proto.metrics.iter().try_for_each(|metric| {
        let proto = metric_proto_to_df(metric.clone())?;
        metrics_set.push(proto);
        Ok::<(), DataFusionError>(())
    })?;
    Ok(metrics_set)
}

/// Custom metrics are not supported in proto conversion.
#[allow(dead_code)]
const CUSTOM_METRICS_NOT_SUPPORTED: &str =
    "custom metrics are not supported in metrics proto conversion";

/// df_metric_to_proto converts a `datafusion::physical_plan::metrics::Metric` to a `MetricProto`. It does not consume the Arc<Metric>.
#[allow(dead_code)]
pub fn df_metric_to_proto(metric: Arc<Metric>) -> Result<MetricProto, DataFusionError> {
    let partition = metric.partition().map(|p| p as u64);
    let labels = metric
        .labels()
        .iter()
        .map(|label| ProtoLabel {
            name: label.name().to_string(),
            value: label.value().to_string(),
        })
        .collect();

    match metric.value() {
        MetricValue::OutputRows(rows) => Ok(MetricProto {
            metric: Some(MetricValueProto::OutputRows(OutputRows { value: rows.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::ElapsedCompute(time) => Ok(MetricProto {
            metric: Some(MetricValueProto::ElapsedCompute(ElapsedCompute { value: time.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpillCount(count) => Ok(MetricProto {
            metric: Some(MetricValueProto::SpillCount(SpillCount { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpilledBytes(count) => Ok(MetricProto {
            metric: Some(MetricValueProto::SpilledBytes(SpilledBytes { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpilledRows(count) => Ok(MetricProto {
            metric: Some(MetricValueProto::SpilledRows(SpilledRows { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::CurrentMemoryUsage(gauge) => Ok(MetricProto {
            metric: Some(MetricValueProto::CurrentMemoryUsage(CurrentMemoryUsage { value: gauge.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::Count { name, count } => Ok(MetricProto {
            metric: Some(MetricValueProto::Count(NamedCount {
                name: name.to_string(),
                value: count.value() as u64
            })),
            partition,
            labels,
        }),
        MetricValue::Gauge { name, gauge } => Ok(MetricProto {
            metric: Some(MetricValueProto::Gauge(NamedGauge {
                name: name.to_string(),
                value: gauge.value() as u64
            })),
            partition,
            labels,
        }),
        MetricValue::Time { name, time } => Ok(MetricProto {
            metric: Some(MetricValueProto::Time(NamedTime {
                name: name.to_string(),
                value: time.value() as u64
            })),
            partition,
            labels,
        }),
        MetricValue::StartTimestamp(timestamp) => Ok(MetricProto {
            metric: Some(MetricValueProto::StartTimestamp(StartTimestamp {
                value: match timestamp.value() {
                    Some(dt) => Some(
                        dt.timestamp_nanos_opt().ok_or(DataFusionError::Internal(
                            "encountered a timestamp which cannot be represented via a nanosecond timestamp".to_string()))?
                    ),
                    None => None,
                },
            })),
            partition,
            labels,
        }),
        MetricValue::EndTimestamp(timestamp) => Ok(MetricProto {
            metric: Some(MetricValueProto::EndTimestamp(EndTimestamp {
                value: match timestamp.value() {
                    Some(dt) => Some(
                        dt.timestamp_nanos_opt().ok_or(DataFusionError::Internal(
                            "encountered a timestamp which cannot be represented via a nanosecond timestamp".to_string()))?
                    ),
                    None => None,
                },
            })),
            partition,
            labels,
        }),
        MetricValue::Custom { .. } => internal_err!("{}", CUSTOM_METRICS_NOT_SUPPORTED),
    }
}

/// metric_proto_to_df converts a `MetricProto` to a `datafusion::physical_plan::metrics::Metric`. It consumes the MetricProto.
pub fn metric_proto_to_df(metric: MetricProto) -> Result<Arc<Metric>, DataFusionError> {
    let partition = metric.partition.map(|p| p as usize);
    let labels = metric
        .labels
        .into_iter()
        .map(|proto_label| Label::new(proto_label.name, proto_label.value))
        .collect();

    match metric.metric {
        Some(MetricValueProto::OutputRows(rows)) => {
            let count = Count::new();
            count.add(rows.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::OutputRows(count),
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::ElapsedCompute(elapsed)) => {
            let time = Time::new();
            time.add_duration(std::time::Duration::from_nanos(elapsed.value));
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::ElapsedCompute(time),
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::SpillCount(spill_count)) => {
            let count = Count::new();
            count.add(spill_count.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::SpillCount(count),
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::SpilledBytes(spilled_bytes)) => {
            let count = Count::new();
            count.add(spilled_bytes.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::SpilledBytes(count),
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::SpilledRows(spilled_rows)) => {
            let count = Count::new();
            count.add(spilled_rows.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::SpilledRows(count),
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::CurrentMemoryUsage(memory)) => {
            let gauge = Gauge::new();
            gauge.set(memory.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::CurrentMemoryUsage(gauge),
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::Count(named_count)) => {
            let count = Count::new();
            count.add(named_count.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Count {
                    name: Cow::Owned(named_count.name),
                    count,
                },
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::Gauge(named_gauge)) => {
            let gauge = Gauge::new();
            gauge.set(named_gauge.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Gauge {
                    name: Cow::Owned(named_gauge.name),
                    gauge,
                },
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::Time(named_time)) => {
            let time = Time::new();
            time.add_duration(std::time::Duration::from_nanos(named_time.value));
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Time {
                    name: Cow::Owned(named_time.name),
                    time,
                },
                partition,
                labels,
            )))
        }
        Some(MetricValueProto::StartTimestamp(start_ts)) => match start_ts.value {
            Some(value) => {
                let timestamp = Timestamp::new();
                timestamp.set(DateTime::from_timestamp_nanos(value));
                Ok(Arc::new(Metric::new_with_labels(
                    MetricValue::StartTimestamp(timestamp),
                    partition,
                    labels,
                )))
            }
            None => internal_err!("encountered invalid start timestamp metric with no value"),
        },
        Some(MetricValueProto::EndTimestamp(end_ts)) => match end_ts.value {
            Some(value) => {
                let timestamp = Timestamp::new();
                timestamp.set(DateTime::from_timestamp_nanos(value));
                Ok(Arc::new(Metric::new_with_labels(
                    MetricValue::EndTimestamp(timestamp),
                    partition,
                    labels,
                )))
            }
            None => internal_err!("encountered invalid end timestamp metric with no value"),
        },
        None => internal_err!("proto metric is missing the metric field"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::CustomMetricValue;
    use datafusion::physical_plan::metrics::{Count, Gauge, Label, MetricsSet, Time, Timestamp};
    use datafusion::physical_plan::metrics::{Metric, MetricValue};
    use std::borrow::Cow;
    use std::sync::Arc;

    fn test_roundtrip_helper(metrics_set: MetricsSet, test_name: &str) {
        // Serialize and deserialize the metrics set.
        let metrics_set_proto = df_metrics_set_to_proto(&metrics_set).unwrap();
        let roundtrip_metrics_set = metrics_set_proto_to_df(&metrics_set_proto).unwrap();

        // Check that we have the same number of metrics.
        let original_count = metrics_set.iter().count();
        let roundtrip_count = roundtrip_metrics_set.iter().count();
        assert_eq!(
            original_count, roundtrip_count,
            "roundtrip should preserve metrics count for {}",
            test_name
        );

        // Verify equivalence of each metric.
        for (original, roundtrip) in metrics_set.iter().zip(roundtrip_metrics_set.iter()) {
            assert_eq!(
                original.partition(),
                roundtrip.partition(),
                "partition mismatch in {}",
                test_name
            );

            assert_eq!(
                original.labels().len(),
                roundtrip.labels().len(),
                "label count mismatch in {}",
                test_name
            );

            for (orig_label, rt_label) in original.labels().iter().zip(roundtrip.labels().iter()) {
                assert_eq!(
                    orig_label.name(),
                    rt_label.name(),
                    "label name mismatch in {}",
                    test_name
                );
                assert_eq!(
                    orig_label.value(),
                    rt_label.value(),
                    "label value mismatch in {}",
                    test_name
                );
            }

            match (original.value(), roundtrip.value()) {
                (MetricValue::OutputRows(orig), MetricValue::OutputRows(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                }
                (MetricValue::ElapsedCompute(orig), MetricValue::ElapsedCompute(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                }
                (MetricValue::SpillCount(orig), MetricValue::SpillCount(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                }
                (MetricValue::SpilledBytes(orig), MetricValue::SpilledBytes(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                }
                (MetricValue::SpilledRows(orig), MetricValue::SpilledRows(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                }
                (MetricValue::CurrentMemoryUsage(orig), MetricValue::CurrentMemoryUsage(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                }
                (
                    MetricValue::Count {
                        name: n1,
                        count: c1,
                    },
                    MetricValue::Count {
                        name: n2,
                        count: c2,
                    },
                ) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    assert_eq!(c1.value(), c2.value());
                }
                (
                    MetricValue::Gauge {
                        name: n1,
                        gauge: g1,
                    },
                    MetricValue::Gauge {
                        name: n2,
                        gauge: g2,
                    },
                ) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    assert_eq!(g1.value(), g2.value());
                }
                (
                    MetricValue::Time { name: n1, time: t1 },
                    MetricValue::Time { name: n2, time: t2 },
                ) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    assert_eq!(t1.value(), t2.value());
                }
                (MetricValue::StartTimestamp(orig), MetricValue::StartTimestamp(rt)) => {
                    assert_eq!(
                        orig.value().map(|dt| dt.timestamp_nanos_opt().unwrap()),
                        rt.value().map(|dt| dt.timestamp_nanos_opt().unwrap())
                    );
                }
                (MetricValue::EndTimestamp(orig), MetricValue::EndTimestamp(rt)) => {
                    assert_eq!(
                        orig.value().map(|dt| dt.timestamp_nanos_opt().unwrap()),
                        rt.value().map(|dt| dt.timestamp_nanos_opt().unwrap())
                    );
                }
                _ => panic!(
                    "mismatched metric types in roundtrip test {}: {:?} vs {:?}",
                    test_name,
                    original.value().name(),
                    roundtrip.value().name()
                ),
            }
        }
    }

    #[test]
    fn test_empty_metrics_roundtrip() {
        let metrics_set = MetricsSet::new();
        test_roundtrip_helper(metrics_set, "empty");
    }

    #[test]
    fn test_output_rows_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let count = Count::new();
        count.add(1234);
        let labels = vec![Label::new("operator", "scan")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::OutputRows(count),
            Some(0),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "output_rows");
    }

    #[test]
    fn test_elapsed_compute_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let time = Time::new();
        time.add_duration(std::time::Duration::from_millis(100));
        let labels = vec![Label::new("stage", "compute")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::ElapsedCompute(time),
            Some(1),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "elapsed_compute");
    }

    #[test]
    fn test_spill_count_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let count = Count::new();
        count.add(456);
        let labels = vec![Label::new("memory", "spill")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::SpillCount(count),
            Some(2),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "spill_count");
    }

    #[test]
    fn test_spilled_bytes_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let count = Count::new();
        count.add(7890);
        let labels = vec![Label::new("disk", "temp")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::SpilledBytes(count),
            Some(3),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "spilled_bytes");
    }

    #[test]
    fn test_spilled_rows_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let count = Count::new();
        count.add(123);
        let labels = vec![Label::new("buffer", "overflow")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::SpilledRows(count),
            Some(4),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "spilled_rows");
    }

    #[test]
    fn test_current_memory_usage_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let gauge = Gauge::new();
        gauge.set(2048);
        let labels = vec![Label::new("resource", "memory")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::CurrentMemoryUsage(gauge),
            Some(5),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "current_memory_usage");
    }

    #[test]
    fn test_named_count_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let count = Count::new();
        count.add(999);
        let labels = vec![
            Label::new("custom", "counter"),
            Label::new("unit", "operations"),
        ];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::Count {
                name: Cow::Borrowed("custom_count"),
                count,
            },
            Some(6),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "named_count");
    }

    #[test]
    fn test_named_gauge_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let gauge = Gauge::new();
        gauge.set(4096);
        let labels = vec![
            Label::new("type", "gauge"),
            Label::new("component", "cache"),
        ];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::Gauge {
                name: Cow::Borrowed("custom_gauge"),
                gauge,
            },
            Some(7),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "named_gauge");
    }

    #[test]
    fn test_named_time_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let time = Time::new();
        time.add_duration(std::time::Duration::from_micros(500));
        let labels = vec![Label::new("phase", "processing")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::Time {
                name: Cow::Borrowed("custom_time"),
                time,
            },
            Some(8),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "named_time");
    }

    #[test]
    fn test_start_timestamp_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let timestamp = Timestamp::new();
        let start_time = DateTime::from_timestamp(1600000000, 0).unwrap();
        timestamp.set(start_time);
        let labels = vec![Label::new("event", "start")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::StartTimestamp(timestamp),
            Some(9),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "start_timestamp");
    }

    #[test]
    fn test_end_timestamp_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let timestamp = Timestamp::new();
        let end_time = DateTime::from_timestamp(1600000100, 0).unwrap();
        timestamp.set(end_time);
        let labels = vec![Label::new("event", "end"), Label::new("duration", "100s")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::EndTimestamp(timestamp),
            Some(10),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "end_timestamp");
    }

    #[test]
    fn test_mixed_metrics_roundtrip() {
        let mut metrics_set = MetricsSet::new();

        let output_count = Count::new();
        output_count.add(1500);
        let output_labels = vec![Label::new("operator", "join"), Label::new("side", "left")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::OutputRows(output_count),
            Some(0),
            output_labels,
        )));

        let compute_time = Time::new();
        compute_time.add_duration(std::time::Duration::from_millis(250));
        let compute_labels = vec![Label::new("phase", "execution")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::ElapsedCompute(compute_time),
            Some(1),
            compute_labels,
        )));

        let memory_gauge = Gauge::new();
        memory_gauge.set(8192);
        let memory_labels = vec![Label::new("resource", "heap"), Label::new("unit", "bytes")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::CurrentMemoryUsage(memory_gauge),
            Some(2),
            memory_labels,
        )));

        let custom_count = Count::new();
        custom_count.add(42);
        let custom_labels = vec![
            Label::new("metric", "custom"),
            Label::new("category", "business"),
        ];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::Count {
                name: Cow::Borrowed("processed_records"),
                count: custom_count,
            },
            Some(3),
            custom_labels,
        )));

        let start_ts = Timestamp::new();
        let start_time = DateTime::from_timestamp(1700000000, 500_000_000).unwrap(); // With nanoseconds
        start_ts.set(start_time);
        let timestamp_labels = vec![
            Label::new("event", "query_start"),
            Label::new("query_id", "abc-123"),
        ];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::StartTimestamp(start_ts),
            Some(4),
            timestamp_labels,
        )));

        test_roundtrip_helper(metrics_set, "mixed_metrics");
    }

    #[test]
    fn test_custom_metrics_filtering() {
        #[derive(Debug)]
        struct TestCustomMetric;

        impl std::fmt::Display for TestCustomMetric {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "test_value")
            }
        }

        impl CustomMetricValue for TestCustomMetric {
            fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
                Arc::new(TestCustomMetric)
            }

            fn aggregate(&self, _other: Arc<dyn CustomMetricValue>) {}

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
                other.as_any().is::<TestCustomMetric>()
            }
        }

        let mut metrics_set = MetricsSet::new();

        // Add a supported metric
        let count = Count::new();
        count.add(100);
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::OutputRows(count),
            Some(0),
        )));

        // Add a custom metric which will be filtered out
        let custom_value = Arc::new(TestCustomMetric);
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: std::borrow::Cow::Borrowed("test_custom"),
                value: custom_value,
            },
            Some(1),
        )));

        let metrics_set_proto = df_metrics_set_to_proto(&metrics_set).unwrap();

        assert_eq!(metrics_set_proto.metrics.len(), 1);

        let remaining_metric = &metrics_set_proto.metrics[0];
        assert!(matches!(
            remaining_metric.metric,
            Some(MetricValueProto::OutputRows(_))
        ));
    }

    #[test]
    fn test_unrepresentable_timestamp_error() {
        // Use a timestamp that is beyond the range that timestamp_nanos_opt() can handle.
        let mut metrics_set = MetricsSet::new();
        let timestamp = Timestamp::new();
        let extreme_time = DateTime::from_timestamp(i64::MAX / 1_000_000_000, 999_999_999).unwrap();
        timestamp.set(extreme_time);
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::StartTimestamp(timestamp),
            Some(0),
        )));

        let proto_result = df_metrics_set_to_proto(&metrics_set);
        assert!(
            proto_result.is_err(),
            "should return error for unrepresentable timestamp"
        );
    }

    #[test]
    fn test_invalid_proto_timestamp_error() {
        // Create a MetricProto with EndTimestamp that has no value (None)
        let invalid_end_timestamp_proto = MetricProto {
            metric: Some(MetricValueProto::EndTimestamp(EndTimestamp { value: None })),
            labels: vec![],
            partition: Some(0),
        };

        let result = metric_proto_to_df(invalid_end_timestamp_proto);
        assert!(
            result.is_err(),
            "should return error for invalid end timestamp with no value"
        );
    }
}
