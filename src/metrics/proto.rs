use chrono::DateTime;
use datafusion::common::internal_err;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::{Count, Gauge, Label, Time, Timestamp};
use datafusion::physical_plan::metrics::{Metric, MetricValue, MetricsSet};
use datafusion::physical_plan::metrics::{PruningMetrics as DfPruningMetrics, RatioMetrics};
use sketches_ddsketch::DDSketch;
use std::borrow::Cow;
use std::sync::Arc;

use super::bytes_metric::BytesCounterMetric;
use super::latency_metric::{
    AvgLatencyMetric, FirstLatencyMetric, MaxLatencyMetric, MinLatencyMetric, P50LatencyMetric,
    P75LatencyMetric, P95LatencyMetric, P99LatencyMetric,
};
use crate::worker::generated::worker as pb;

/// df_metrics_set_to_proto converts a [datafusion::physical_plan::metrics::MetricsSet] to a [pb::MetricsSet].
/// Custom metrics are filtered out, but any other errors are returned.
/// TODO(#140): Support custom metrics.
pub fn df_metrics_set_to_proto(
    metrics_set: &MetricsSet,
) -> Result<pb::MetricsSet, DataFusionError> {
    let mut metrics = Vec::new();

    for metric in metrics_set.iter() {
        match df_metric_to_proto(metric.clone()) {
            Ok(metric_proto) => metrics.push(metric_proto),
            Err(err) => {
                // Check if this is the specific custom metrics error we want to filter out
                if let DataFusionError::Internal(msg) = &err {
                    if msg == CUSTOM_METRICS_NOT_SUPPORTED || msg == UNSUPPORTED_METRICS {
                        // Filter out custom/unsupported metrics error - continue processing other metrics
                        continue;
                    }
                }
                // Any other error should be returned
                return Err(err);
            }
        }
    }

    Ok(pb::MetricsSet { metrics })
}

/// metrics_set_proto_to_df converts a [pb::MetricsSet] to a [datafusion::physical_plan::metrics::MetricsSet].
pub fn metrics_set_proto_to_df(
    metrics_set_proto: &pb::MetricsSet,
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
const CUSTOM_METRICS_NOT_SUPPORTED: &str =
    "custom metrics are not supported in metrics proto conversion";

/// New DataFusion metrics that are not yet supported in proto conversion.
const UNSUPPORTED_METRICS: &str = "metric type not supported in proto conversion";

/// df_metric_to_proto converts a `datafusion::physical_plan::metrics::Metric` to a `pb::Metric`. It does not consume the Arc<Metric>.
pub fn df_metric_to_proto(metric: Arc<Metric>) -> Result<pb::Metric, DataFusionError> {
    let partition = metric.partition().map(|p| p as u64);
    let labels = metric
        .labels()
        .iter()
        .map(|label| pb::Label {
            name: label.name().to_string(),
            value: label.value().to_string(),
        })
        .collect();

    match metric.value() {
        MetricValue::OutputRows(rows) => Ok(pb::Metric {
            value: Some(pb::metric::Value::OutputRows(pb::OutputRows { value: rows.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::ElapsedCompute(time) => Ok(pb::Metric {
            value: Some(pb::metric::Value::ElapsedCompute(pb::ElapsedCompute { value: time.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpillCount(count) => Ok(pb::Metric {
            value: Some(pb::metric::Value::SpillCount(pb::SpillCount { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpilledBytes(count) => Ok(pb::Metric {
            value: Some(pb::metric::Value::SpilledBytes(pb::SpilledBytes { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpilledRows(count) => Ok(pb::Metric {
            value: Some(pb::metric::Value::SpilledRows(pb::SpilledRows { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::CurrentMemoryUsage(gauge) => Ok(pb::Metric {
            value: Some(pb::metric::Value::CurrentMemoryUsage(pb::CurrentMemoryUsage { value: gauge.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::Count { name, count } => Ok(pb::Metric {
            value: Some(pb::metric::Value::Count(pb::NamedCount {
                name: name.to_string(),
                value: count.value() as u64
            })),
            partition,
            labels,
        }),
        MetricValue::Gauge { name, gauge } => Ok(pb::Metric {
            value: Some(pb::metric::Value::Gauge(pb::NamedGauge {
                name: name.to_string(),
                value: gauge.value() as u64
            })),
            partition,
            labels,
        }),
        MetricValue::Time { name, time } => Ok(pb::Metric {
            value: Some(pb::metric::Value::Time(pb::NamedTime {
                name: name.to_string(),
                value: time.value() as u64
            })),
            partition,
            labels,
        }),
        MetricValue::StartTimestamp(timestamp) => Ok(pb::Metric {
            value: Some(pb::metric::Value::StartTimestamp(pb::StartTimestamp {
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
        MetricValue::EndTimestamp(timestamp) => Ok(pb::Metric {
            value: Some(pb::metric::Value::EndTimestamp(pb::EndTimestamp {
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
        MetricValue::Custom { name, value } => {
            if let Some(min) = value.as_any().downcast_ref::<MinLatencyMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomMinLatency(pb::MinLatency {
                        name: name.to_string(),
                        value: min.value() as u64,
                    })),
                    partition,
                    labels,
                })
            } else if let Some(max) = value.as_any().downcast_ref::<MaxLatencyMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomMaxLatency(pb::MaxLatency {
                        name: name.to_string(),
                        value: max.value() as u64,
                    })),
                    partition,
                    labels,
                })
            } else if let Some(avg) = value.as_any().downcast_ref::<AvgLatencyMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomAvgLatency(pb::AvgLatency {
                        name: name.to_string(),
                        nanos_sum: avg.nanos_sum() as u64,
                        count: avg.count() as u64,
                    })),
                    partition,
                    labels,
                })
            } else if let Some(first) = value.as_any().downcast_ref::<FirstLatencyMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomFirstLatency(pb::FirstLatency {
                        name: name.to_string(),
                        value: first.value() as u64,
                    })),
                    partition,
                    labels,
                })
            } else if let Some(bytes) = value.as_any().downcast_ref::<BytesCounterMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomBytesCount(pb::BytesCount {
                        name: name.to_string(),
                        value: bytes.value() as u64,
                    })),
                    partition,
                    labels,
                })
            } else if let Some(p50) = value.as_any().downcast_ref::<P50LatencyMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomP50Latency(pb::PercentileLatency {
                        name: name.to_string(),
                        sketch_bytes: p50.serialize_sketch()?,
                    })),
                    partition,
                    labels,
                })
            } else if let Some(p75) = value.as_any().downcast_ref::<P75LatencyMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomP75Latency(pb::PercentileLatency {
                        name: name.to_string(),
                        sketch_bytes: p75.serialize_sketch()?,
                    })),
                    partition,
                    labels,
                })
            } else if let Some(p95) = value.as_any().downcast_ref::<P95LatencyMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomP95Latency(pb::PercentileLatency {
                        name: name.to_string(),
                        sketch_bytes: p95.serialize_sketch()?,
                    })),
                    partition,
                    labels,
                })
            } else if let Some(p99) = value.as_any().downcast_ref::<P99LatencyMetric>() {
                Ok(pb::Metric {
                    value: Some(pb::metric::Value::CustomP99Latency(pb::PercentileLatency {
                        name: name.to_string(),
                        sketch_bytes: p99.serialize_sketch()?,
                    })),
                    partition,
                    labels,
                })
            } else {
                internal_err!("{}", CUSTOM_METRICS_NOT_SUPPORTED)
            }
        }
        MetricValue::OutputBytes(count) => Ok(pb::Metric {
            value: Some(pb::metric::Value::OutputBytes(pb::OutputBytes { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::OutputBatches(count) => Ok(pb::Metric {
            value: Some(pb::metric::Value::OutputBatches(pb::OutputBatches { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::PruningMetrics { name, pruning_metrics } => Ok(pb::Metric {
            value: Some(pb::metric::Value::PruningMetrics(pb::NamedPruningMetrics {
                name: name.to_string(),
                pruned: pruning_metrics.pruned() as u64,
                matched: pruning_metrics.matched() as u64,
            })),
            partition,
            labels,
        }),
        MetricValue::Ratio { name, ratio_metrics } => Ok(pb::Metric {
            value: Some(pb::metric::Value::Ratio(pb::NamedRatio {
                name: name.to_string(),
                part: ratio_metrics.part() as u64,
                total: ratio_metrics.total() as u64,
            })),
            partition,
            labels,
        }),
    }
}

/// metric_proto_to_df converts a `pb::Metric` to a `datafusion::physical_plan::metrics::Metric`. It consumes the pb::Metric.
pub fn metric_proto_to_df(metric: pb::Metric) -> Result<Arc<Metric>, DataFusionError> {
    let partition = metric.partition.map(|p| p as usize);
    let labels = metric
        .labels
        .into_iter()
        .map(|proto_label| Label::new(proto_label.name, proto_label.value))
        .collect();

    match metric.value {
        Some(pb::metric::Value::OutputRows(rows)) => {
            let count = Count::new();
            count.add(rows.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::OutputRows(count),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::ElapsedCompute(elapsed)) => {
            let time = Time::new();
            time.add_duration(std::time::Duration::from_nanos(elapsed.value));
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::ElapsedCompute(time),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::SpillCount(spill_count)) => {
            let count = Count::new();
            count.add(spill_count.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::SpillCount(count),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::SpilledBytes(spilled_bytes)) => {
            let count = Count::new();
            count.add(spilled_bytes.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::SpilledBytes(count),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::SpilledRows(spilled_rows)) => {
            let count = Count::new();
            count.add(spilled_rows.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::SpilledRows(count),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CurrentMemoryUsage(memory)) => {
            let gauge = Gauge::new();
            gauge.set(memory.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::CurrentMemoryUsage(gauge),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::Count(named_count)) => {
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
        Some(pb::metric::Value::Gauge(named_gauge)) => {
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
        Some(pb::metric::Value::Time(named_time)) => {
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
        Some(pb::metric::Value::StartTimestamp(start_ts)) => {
            let timestamp = Timestamp::new();
            if let Some(value) = start_ts.value {
                timestamp.set(DateTime::from_timestamp_nanos(value));
            }
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::StartTimestamp(timestamp),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::EndTimestamp(end_ts)) => {
            let timestamp = Timestamp::new();
            if let Some(value) = end_ts.value {
                timestamp.set(DateTime::from_timestamp_nanos(value));
            }
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::EndTimestamp(timestamp),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::OutputBytes(output_bytes)) => {
            let count = Count::new();
            count.add(output_bytes.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::OutputBytes(count),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::OutputBatches(output_batches)) => {
            let count = Count::new();
            count.add(output_batches.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::OutputBatches(count),
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::PruningMetrics(named_pruning)) => {
            let pruning_metrics = DfPruningMetrics::new();
            pruning_metrics.add_pruned(named_pruning.pruned as usize);
            pruning_metrics.add_matched(named_pruning.matched as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::PruningMetrics {
                    name: Cow::Owned(named_pruning.name),
                    pruning_metrics,
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::Ratio(named_ratio)) => {
            let ratio_metrics = RatioMetrics::new();
            ratio_metrics.set_part(named_ratio.part as usize);
            ratio_metrics.set_total(named_ratio.total as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Ratio {
                    name: Cow::Owned(named_ratio.name),
                    ratio_metrics,
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomMinLatency(min_latency)) => {
            let value = MinLatencyMetric::from_nanos(min_latency.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(min_latency.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomMaxLatency(max_latency)) => {
            let value = MaxLatencyMetric::from_nanos(max_latency.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(max_latency.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomAvgLatency(avg_latency)) => {
            let value = AvgLatencyMetric::from_raw(
                avg_latency.nanos_sum as usize,
                avg_latency.count as usize,
            );
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(avg_latency.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomFirstLatency(first_latency)) => {
            let value = FirstLatencyMetric::from_nanos(first_latency.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(first_latency.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomBytesCount(bytes_count)) => {
            let value = BytesCounterMetric::from_value(bytes_count.value as usize);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(bytes_count.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomP50Latency(p)) => {
            let sketch: DDSketch = bincode::deserialize(&p.sketch_bytes).map_err(|e| {
                DataFusionError::Internal(format!("failed to deserialize DDSketch: {e}"))
            })?;
            let value = P50LatencyMetric::from_sketch(sketch);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(p.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomP75Latency(p)) => {
            let sketch: DDSketch = bincode::deserialize(&p.sketch_bytes).map_err(|e| {
                DataFusionError::Internal(format!("failed to deserialize DDSketch: {e}"))
            })?;
            let value = P75LatencyMetric::from_sketch(sketch);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(p.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomP95Latency(p)) => {
            let sketch: DDSketch = bincode::deserialize(&p.sketch_bytes).map_err(|e| {
                DataFusionError::Internal(format!("failed to deserialize DDSketch: {e}"))
            })?;
            let value = P95LatencyMetric::from_sketch(sketch);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(p.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        Some(pb::metric::Value::CustomP99Latency(p)) => {
            let sketch: DDSketch = bincode::deserialize(&p.sketch_bytes).map_err(|e| {
                DataFusionError::Internal(format!("failed to deserialize DDSketch: {e}"))
            })?;
            let value = P99LatencyMetric::from_sketch(sketch);
            Ok(Arc::new(Metric::new_with_labels(
                MetricValue::Custom {
                    name: Cow::Owned(p.name),
                    value: Arc::new(value),
                },
                partition,
                labels,
            )))
        }
        None => internal_err!("proto metric is missing the metric field"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::CustomMetricValue;
    use datafusion::physical_plan::metrics::{Count, Gauge, Label, MetricsSet, Time, Timestamp};
    use datafusion::physical_plan::metrics::{Metric, MetricValue};
    use datafusion::physical_plan::metrics::{PruningMetrics as DfPruningMetrics, RatioMetrics};
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
            "roundtrip should preserve metrics count for {test_name}"
        );

        // Verify equivalence of each metric.
        for (original, roundtrip) in metrics_set.iter().zip(roundtrip_metrics_set.iter()) {
            assert_eq!(
                original.partition(),
                roundtrip.partition(),
                "partition mismatch in {test_name}"
            );

            assert_eq!(
                original.labels().len(),
                roundtrip.labels().len(),
                "label count mismatch in {test_name}"
            );

            for (orig_label, rt_label) in original.labels().iter().zip(roundtrip.labels().iter()) {
                assert_eq!(
                    orig_label.name(),
                    rt_label.name(),
                    "label name mismatch in {test_name}"
                );
                assert_eq!(
                    orig_label.value(),
                    rt_label.value(),
                    "label value mismatch in {test_name}"
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
                (MetricValue::OutputBytes(orig), MetricValue::OutputBytes(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                }
                (MetricValue::OutputBatches(orig), MetricValue::OutputBatches(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                }
                (
                    MetricValue::PruningMetrics {
                        name: n1,
                        pruning_metrics: p1,
                    },
                    MetricValue::PruningMetrics {
                        name: n2,
                        pruning_metrics: p2,
                    },
                ) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    assert_eq!(p1.pruned(), p2.pruned());
                    assert_eq!(p1.matched(), p2.matched());
                }
                (
                    MetricValue::Ratio {
                        name: n1,
                        ratio_metrics: r1,
                    },
                    MetricValue::Ratio {
                        name: n2,
                        ratio_metrics: r2,
                    },
                ) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    assert_eq!(r1.part(), r2.part());
                    assert_eq!(r1.total(), r2.total());
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
            remaining_metric.value,
            Some(pb::metric::Value::OutputRows(_))
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
    fn test_default_timestamp_roundtrip() {
        let default_timestamp = Timestamp::default();
        let metric_with_default_timestamp =
            Metric::new(MetricValue::EndTimestamp(default_timestamp), Some(0));

        let proto_result = df_metric_to_proto(Arc::new(metric_with_default_timestamp));
        assert!(
            proto_result.is_ok(),
            "should successfully convert default timestamp to proto"
        );

        let proto_metric = proto_result.unwrap();
        let roundtrip_result = metric_proto_to_df(proto_metric);
        assert!(
            roundtrip_result.is_ok(),
            "should successfully roundtrip default timestamp"
        );
    }

    #[test]
    fn test_output_bytes_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let count = Count::new();
        count.add(8192);
        let labels = vec![Label::new("source", "parquet")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::OutputBytes(count),
            Some(0),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "output_bytes");
    }

    #[test]
    fn test_output_batches_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let count = Count::new();
        count.add(42);
        let labels = vec![Label::new("operator", "filter")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::OutputBatches(count),
            Some(1),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "output_batches");
    }

    #[test]
    fn test_pruning_metrics_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let pruning_metrics = DfPruningMetrics::new();
        pruning_metrics.add_pruned(100);
        pruning_metrics.add_matched(50);
        let labels = vec![Label::new("predicate", "range")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::PruningMetrics {
                name: Cow::Borrowed("row_groups"),
                pruning_metrics,
            },
            Some(2),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "pruning_metrics");
    }

    #[test]
    fn test_ratio_metrics_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        let ratio_metrics = RatioMetrics::new();
        ratio_metrics.set_part(75);
        ratio_metrics.set_total(100);
        let labels = vec![Label::new("type", "cache_hit")];
        metrics_set.push(Arc::new(Metric::new_with_labels(
            MetricValue::Ratio {
                name: Cow::Borrowed("cache_hit_ratio"),
                ratio_metrics,
            },
            Some(3),
            labels,
        )));
        test_roundtrip_helper(metrics_set, "ratio_metrics");
    }

    #[test]
    fn test_latency_metrics_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("min_latency"),
                value: Arc::new(MinLatencyMetric::from_nanos(10_000)),
            },
            Some(0),
        )));
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("max_latency"),
                value: Arc::new(MaxLatencyMetric::from_nanos(90_000)),
            },
            Some(0),
        )));
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("avg_latency"),
                value: Arc::new(AvgLatencyMetric::from_raw(300_000, 3)),
            },
            Some(0),
        )));
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("first_latency"),
                value: Arc::new(FirstLatencyMetric::from_nanos(55_000)),
            },
            Some(0),
        )));

        // Build percentile metrics by adding sample durations
        let p50 = P50LatencyMetric::default();
        let p75 = P75LatencyMetric::default();
        let p95 = P95LatencyMetric::default();
        let p99 = P99LatencyMetric::default();
        for _ in 0..100 {
            let d = std::time::Duration::from_millis(10);
            p50.add_duration(d);
            p75.add_duration(d);
            p95.add_duration(d);
            p99.add_duration(d);
        }
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("p50_latency"),
                value: Arc::new(p50),
            },
            Some(0),
        )));
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("p75_latency"),
                value: Arc::new(p75),
            },
            Some(0),
        )));
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("p95_latency"),
                value: Arc::new(p95),
            },
            Some(0),
        )));
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("p99_latency"),
                value: Arc::new(p99),
            },
            Some(0),
        )));

        let proto = df_metrics_set_to_proto(&metrics_set).unwrap();
        assert_eq!(proto.metrics.len(), 8);

        let rt = metrics_set_proto_to_df(&proto).unwrap();
        assert_eq!(rt.iter().count(), 8);

        for (orig, rt) in metrics_set.iter().zip(rt.iter()) {
            match (orig.value(), rt.value()) {
                (
                    MetricValue::Custom {
                        name: n1,
                        value: v1,
                    },
                    MetricValue::Custom {
                        name: n2,
                        value: v2,
                    },
                ) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    if let Some(v1) = v1.as_any().downcast_ref::<MinLatencyMetric>() {
                        let v2 = v2.as_any().downcast_ref::<MinLatencyMetric>().unwrap();
                        assert_eq!(v1.value(), v2.value());
                    } else if let Some(v1) = v1.as_any().downcast_ref::<MaxLatencyMetric>() {
                        let v2 = v2.as_any().downcast_ref::<MaxLatencyMetric>().unwrap();
                        assert_eq!(v1.value(), v2.value());
                    } else if let Some(v1) = v1.as_any().downcast_ref::<AvgLatencyMetric>() {
                        let v2 = v2.as_any().downcast_ref::<AvgLatencyMetric>().unwrap();
                        assert_eq!(v1.nanos_sum(), v2.nanos_sum());
                        assert_eq!(v1.count(), v2.count());
                    } else if let Some(v1) = v1.as_any().downcast_ref::<FirstLatencyMetric>() {
                        let v2 = v2.as_any().downcast_ref::<FirstLatencyMetric>().unwrap();
                        assert_eq!(v1.value(), v2.value());
                    } else if let Some(v1) = v1.as_any().downcast_ref::<P50LatencyMetric>() {
                        let v2 = v2.as_any().downcast_ref::<P50LatencyMetric>().unwrap();
                        assert_eq!(v1.value(), v2.value());
                        assert_eq!(v1.count(), v2.count());
                    } else if let Some(v1) = v1.as_any().downcast_ref::<P75LatencyMetric>() {
                        let v2 = v2.as_any().downcast_ref::<P75LatencyMetric>().unwrap();
                        assert_eq!(v1.value(), v2.value());
                        assert_eq!(v1.count(), v2.count());
                    } else if let Some(v1) = v1.as_any().downcast_ref::<P95LatencyMetric>() {
                        let v2 = v2.as_any().downcast_ref::<P95LatencyMetric>().unwrap();
                        assert_eq!(v1.value(), v2.value());
                        assert_eq!(v1.count(), v2.count());
                    } else if let Some(v1) = v1.as_any().downcast_ref::<P99LatencyMetric>() {
                        let v2 = v2.as_any().downcast_ref::<P99LatencyMetric>().unwrap();
                        assert_eq!(v1.value(), v2.value());
                        assert_eq!(v1.count(), v2.count());
                    } else {
                        panic!("unexpected custom metric type");
                    }
                }
                _ => panic!("expected Custom metrics"),
            }
        }
    }

    #[test]
    fn test_bytes_counter_metric_roundtrip() {
        let mut metrics_set = MetricsSet::new();
        metrics_set.push(Arc::new(Metric::new(
            MetricValue::Custom {
                name: Cow::Borrowed("bytes_transferred"),
                value: Arc::new(BytesCounterMetric::from_value(1_073_741_824)),
            },
            Some(0),
        )));

        let proto = df_metrics_set_to_proto(&metrics_set).unwrap();
        assert_eq!(proto.metrics.len(), 1);

        let rt = metrics_set_proto_to_df(&proto).unwrap();
        assert_eq!(rt.iter().count(), 1);

        let orig = metrics_set.iter().next().unwrap();
        let rt = rt.iter().next().unwrap();

        match (orig.value(), rt.value()) {
            (
                MetricValue::Custom {
                    name: n1,
                    value: v1,
                },
                MetricValue::Custom {
                    name: n2,
                    value: v2,
                },
            ) => {
                assert_eq!(n1.as_ref(), n2.as_ref());
                let v1 = v1.as_any().downcast_ref::<BytesCounterMetric>().unwrap();
                let v2 = v2.as_any().downcast_ref::<BytesCounterMetric>().unwrap();
                assert_eq!(v1.value(), v2.value());
            }
            _ => panic!("expected Custom metrics"),
        }
    }
}
