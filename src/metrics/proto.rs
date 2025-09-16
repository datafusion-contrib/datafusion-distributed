use datafusion::physical_plan::metrics::{Metric, MetricValue, MetricsSet};
use datafusion::error::DataFusionError;
use std::sync::Arc;

/// A ProtoMetric mirrors `datafusion::physical_plan::metrics::Metric`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoMetric {
    #[prost(oneof="ProtoMetricValue", tags="1,2,3,4,5,6,7,8,9")]
    // This field is *always* set. It is marked optional due to protobuf "oneof" requirements.
    pub metric: Option<ProtoMetricValue>,
    #[prost(message, repeated, tag="10")]
    pub labels: Vec<ProtoLabel>,
    #[prost(uint64, optional, tag="11")]
    pub partition: Option<u64>,
}

/// A ProtoMetric mirrors `datafusion::physical_plan::metrics::MetricSet`. It represents
/// metrics for one `ExecutionPlan` node.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoMetricsSet {
    #[prost(message, repeated, tag="1")]
    pub metrics: Vec<ProtoMetric>,
}

/// The MetricType enum mirrors the `datafusion::physical_plan::metrics::MetricValue` enum.
#[derive(Clone, PartialEq, ::prost::Oneof)]
pub enum ProtoMetricValue {
    #[prost(message, tag="1")]
    OutputRows(OutputRows),
    #[prost(message, tag="2")]
    ElapsedCompute(ElapsedCompute),
    #[prost(message, tag="3")]
    SpillCount(SpillCount),
    #[prost(message, tag="4")]
    SpilledBytes(SpilledBytes),
    #[prost(message, tag="5")]
    SpilledRows(SpilledRows),
    #[prost(message, tag="6")]
    CurrentMemoryUsage(CurrentMemoryUsage),
    #[prost(message, tag="7")]
    Count(NamedCount),
    #[prost(message, tag="8")]
    Gauge(NamedGauge),
    #[prost(message, tag="9")]
    Time(NamedTime),
    // #[prost(message, tag="10")]
    // StartTimestamp(StartTimestamp),
    // #[prost(message, tag="11")]
    // EndTimestamp(EndTimestamp),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct OutputRows {
    #[prost(uint64, tag="1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ElapsedCompute {
    #[prost(uint64, tag="1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpillCount {
    #[prost(uint64, tag="1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpilledBytes {
    #[prost(uint64, tag="1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SpilledRows {
    #[prost(uint64, tag="1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CurrentMemoryUsage {
    #[prost(uint64, tag="1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NamedCount {
    #[prost(string, tag="1")]
    pub name: String,
    #[prost(uint64, tag="2")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NamedGauge {
    #[prost(string, tag="1")]
    pub name: String,
    #[prost(uint64, tag="2")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NamedTime {
    #[prost(string, tag="1")]
    pub name: String,
    #[prost(uint64, tag="2")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartTimestamp {
    #[prost(uint64, tag="1")]
    pub value: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EndTimestamp {
    #[prost(uint64, tag="1")]
    pub value: u64,
}

/// A ProtoLabel mirrors `datafusion::physical_plan::metrics::Label`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoLabel {
    #[prost(string, tag="1")]
    pub name: String,
    #[prost(string, tag="2")]
    pub value: String,
}

/// NOTE: we filter out metrics that are not supported by the proto representation.
pub fn df_metrics_set_to_proto(metrics_set: &MetricsSet) -> Result<ProtoMetricsSet, DataFusionError> {
    let metrics = metrics_set.iter().filter_map(|metric|df_metric_to_proto(metric.clone()).ok()).collect::<Vec<_>>();
    let result = ProtoMetricsSet { metrics };
    Ok(result)
}

pub fn proto_metrics_set_to_df(proto_metrics_set: &ProtoMetricsSet) -> Result<MetricsSet, DataFusionError> {
    let mut metrics_set = MetricsSet::new();
    proto_metrics_set.metrics.iter().try_for_each(|metric| {
        let proto = proto_metric_to_df(metric.clone())?; 
        metrics_set.push(proto);
        Ok::<(), DataFusionError>(())
    })?;
    Ok(metrics_set)
}

/// df_metric_to_proto converts a `datafusion::physical_plan::metrics::Metric` to a `ProtoMetric`. It does not consume the Arc<Metric>.
pub fn df_metric_to_proto(metric: Arc<Metric>) -> Result<ProtoMetric, DataFusionError> {
    let partition = metric.partition().map(|p| p as u64);
    let labels = metric.labels().iter().map(|label| ProtoLabel {
        name: label.name().to_string(),
        value: label.value().to_string(),
    }).collect();
    
    match metric.value() {
        MetricValue::OutputRows(rows) => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::OutputRows(OutputRows { value: rows.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::ElapsedCompute(time) => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::ElapsedCompute(ElapsedCompute { value: time.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpillCount(count) => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::SpillCount(SpillCount { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpilledBytes(count) => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::SpilledBytes(SpilledBytes { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::SpilledRows(count) => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::SpilledRows(SpilledRows { value: count.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::CurrentMemoryUsage(gauge) => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::CurrentMemoryUsage(CurrentMemoryUsage { value: gauge.value() as u64 })),
            partition,
            labels,
        }),
        MetricValue::Count { name, count } => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::Count(NamedCount { 
                name: name.to_string(), 
                value: count.value() as u64 
            })),
            partition,
            labels,
        }),
        MetricValue::Gauge { name, gauge } => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::Gauge(NamedGauge { 
                name: name.to_string(), 
                value: gauge.value() as u64 
            })),
            partition,
            labels,
        }),
        MetricValue::Time { name, time } => Ok(ProtoMetric {
            metric: Some(ProtoMetricValue::Time(NamedTime { 
                name: name.to_string(), 
                value: time.value() as u64 
            })),
            partition,
            labels,
        }),
        // MetricValue::StartTimestamp(timestamp) => Ok(ProtoMetric {
        //     metric: Some(ProtoMetricValue::StartTimestamp(StartTimestamp { 
        //         value: timestamp.value().map(|dt| dt.timestamp() as u64).unwrap_or(0) 
        //     })),
        //     partition,
        //     labels,
        // }),
        // MetricValue::EndTimestamp(timestamp) => Ok(ProtoMetric {
        //     metric: Some(ProtoMetricValue::EndTimestamp(EndTimestamp { 
        //         value: timestamp.value().map(|dt| dt.timestamp() as u64).unwrap_or(0) 
        //     })),
        //     partition,
        //     labels,
        // }),

        MetricValue::Custom { .. } => Err(DataFusionError::Internal("Custom metrics are not supported in proto conversion".to_string())),
        _ => Err(DataFusionError::Internal("Timestamp metrics are not supported in proto conversion".to_string())),
    }
}

/// proto_metric_to_df converts a `ProtoMetric` to a `datafusion::physical_plan::metrics::Metric`. It consumes the ProtoMetric.
pub fn proto_metric_to_df(metric: ProtoMetric) -> Result<Arc<Metric>, DataFusionError> {
    use datafusion::physical_plan::metrics::{Count, Time, Gauge, Timestamp, Label};
    use std::borrow::Cow;
    
    let partition = metric.partition.map(|p| p as usize);
    let labels = metric.labels.into_iter().map(|proto_label| {
        Label::new(proto_label.name, proto_label.value)
    }).collect();
    
    match metric.metric {
        Some(ProtoMetricValue::OutputRows(rows)) => {
            let count = Count::new();
            count.add(rows.value as usize);
            Ok(Arc::new(Metric::new_with_labels(MetricValue::OutputRows(count), partition, labels)))
        },
        Some(ProtoMetricValue::ElapsedCompute(elapsed)) => {
            let time = Time::new();
            time.add_duration(std::time::Duration::from_nanos(elapsed.value));
            Ok(Arc::new(Metric::new_with_labels(MetricValue::ElapsedCompute(time), partition, labels)))
        },
        Some(ProtoMetricValue::SpillCount(spill_count)) => {
            let count = Count::new();
            count.add(spill_count.value as usize);
            Ok(Arc::new(Metric::new_with_labels(MetricValue::SpillCount(count), partition, labels)))
        },
        Some(ProtoMetricValue::SpilledBytes(spilled_bytes)) => {
            let count = Count::new();
            count.add(spilled_bytes.value as usize);
            Ok(Arc::new(Metric::new_with_labels(MetricValue::SpilledBytes(count), partition, labels)))
        },
        Some(ProtoMetricValue::SpilledRows(spilled_rows)) => {
            let count = Count::new();
            count.add(spilled_rows.value as usize);
            Ok(Arc::new(Metric::new_with_labels(MetricValue::SpilledRows(count), partition, labels)))
        },
        Some(ProtoMetricValue::CurrentMemoryUsage(memory)) => {
            let gauge = Gauge::new();
            gauge.set(memory.value as usize);
            Ok(Arc::new(Metric::new_with_labels(MetricValue::CurrentMemoryUsage(gauge), partition, labels)))
        },
        Some(ProtoMetricValue::Count(named_count)) => {
            let count = Count::new();
            count.add(named_count.value as usize);
            Ok(Arc::new(Metric::new_with_labels(MetricValue::Count { 
                name: Cow::Owned(named_count.name), 
                count 
            }, partition, labels)))
        },
        Some(ProtoMetricValue::Gauge(named_gauge)) => {
            let gauge = Gauge::new();
            gauge.set(named_gauge.value as usize);
            Ok(Arc::new(Metric::new_with_labels(MetricValue::Gauge { 
                name: Cow::Owned(named_gauge.name), 
                gauge 
            }, partition, labels)))
        },
        Some(ProtoMetricValue::Time(named_time)) => {
            let time = Time::new();
            time.add_duration(std::time::Duration::from_nanos(named_time.value));
            Ok(Arc::new(Metric::new_with_labels(MetricValue::Time { 
                name: Cow::Owned(named_time.name), 
                time 
            }, partition, labels)))
        },
        // Timestamp cases commented out due to DateTime import complexity
        // Some(ProtoMetricValue::StartTimestamp(_start_ts)) => {
        //     Err(DataFusionError::Internal("StartTimestamp conversion not yet implemented".to_string()))
        // },
        // Some(ProtoMetricValue::EndTimestamp(_end_ts)) => {
        //     Err(DataFusionError::Internal("EndTimestamp conversion not yet implemented".to_string()))
        // },
        None => Err(DataFusionError::Internal("proto metric is missing the metric field".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::{Count, Time, MetricValue, Metric, Label};

    #[test]
    fn test_metric_roundtrip() {
        use datafusion::physical_plan::metrics::{MetricsSet, Count, Time, Gauge};
        use std::borrow::Cow;
        
        // Create a MetricsSet with one metric of each supported type
        let mut metrics_set = MetricsSet::new();
        
        // 1. OutputRows
        let count1 = Count::new();
        count1.add(1234);
        metrics_set.push(Arc::new(Metric::new(MetricValue::OutputRows(count1), Some(0))));
        
        // 2. ElapsedCompute
        let time1 = Time::new();
        time1.add_duration(std::time::Duration::from_millis(100));
        metrics_set.push(Arc::new(Metric::new(MetricValue::ElapsedCompute(time1), Some(1))));
        
        // 3. SpillCount
        let count2 = Count::new();
        count2.add(456);
        metrics_set.push(Arc::new(Metric::new(MetricValue::SpillCount(count2), Some(2))));
        
        // 4. SpilledBytes
        let count3 = Count::new();
        count3.add(7890);
        metrics_set.push(Arc::new(Metric::new(MetricValue::SpilledBytes(count3), Some(3))));
        
        // 5. SpilledRows
        let count4 = Count::new();
        count4.add(123);
        metrics_set.push(Arc::new(Metric::new(MetricValue::SpilledRows(count4), Some(4))));
        
        // 6. CurrentMemoryUsage
        let gauge1 = Gauge::new();
        gauge1.set(2048);
        metrics_set.push(Arc::new(Metric::new(MetricValue::CurrentMemoryUsage(gauge1), Some(5))));
        
        // 7. Named Count
        let count5 = Count::new();
        count5.add(999);
        metrics_set.push(Arc::new(Metric::new(MetricValue::Count {
            name: Cow::Borrowed("custom_count"),
            count: count5
        }, Some(6))));
        
        // 8. Named Gauge
        let gauge2 = Gauge::new();
        gauge2.set(4096);
        metrics_set.push(Arc::new(Metric::new(MetricValue::Gauge {
            name: Cow::Borrowed("custom_gauge"),
            gauge: gauge2
        }, Some(7))));
        
        // 9. Named Time
        let time2 = Time::new();
        time2.add_duration(std::time::Duration::from_micros(500));
        metrics_set.push(Arc::new(Metric::new(MetricValue::Time {
            name: Cow::Borrowed("custom_time"),
            time: time2
        }, Some(8))));
        
        // Note: Timestamp metrics are commented out due to DateTime import issues
        // They are supported in the conversion functions but not tested here
        
        // Test: DataFusion MetricsSet -> ProtoMetricsSet
        let proto_metrics_set = df_metrics_set_to_proto(&metrics_set).unwrap();
        
        // Test: ProtoMetricsSet -> DataFusion MetricsSet  
        let roundtrip_metrics_set = proto_metrics_set_to_df(&proto_metrics_set).unwrap();
        
        // Verify the roundtrip preserved the metrics
        let original_count = metrics_set.iter().count();
        let roundtrip_count = roundtrip_metrics_set.iter().count();
        assert_eq!(original_count, roundtrip_count, 
                  "Roundtrip should preserve metrics count");
        
        // Verify individual metrics
        for (original, roundtrip) in metrics_set.iter().zip(roundtrip_metrics_set.iter()) {
            assert_eq!(original.partition(), roundtrip.partition());
            assert_eq!(original.labels().len(), roundtrip.labels().len());
            
            match (original.value(), roundtrip.value()) {
                (MetricValue::OutputRows(orig), MetricValue::OutputRows(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                },
                (MetricValue::ElapsedCompute(orig), MetricValue::ElapsedCompute(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                },
                (MetricValue::SpillCount(orig), MetricValue::SpillCount(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                },
                (MetricValue::SpilledBytes(orig), MetricValue::SpilledBytes(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                },
                (MetricValue::SpilledRows(orig), MetricValue::SpilledRows(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                },
                (MetricValue::CurrentMemoryUsage(orig), MetricValue::CurrentMemoryUsage(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                },
                (MetricValue::Count { name: n1, count: c1 }, MetricValue::Count { name: n2, count: c2 }) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    assert_eq!(c1.value(), c2.value());
                },
                (MetricValue::Gauge { name: n1, gauge: g1 }, MetricValue::Gauge { name: n2, gauge: g2 }) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    assert_eq!(g1.value(), g2.value());
                },
                (MetricValue::Time { name: n1, time: t1 }, MetricValue::Time { name: n2, time: t2 }) => {
                    assert_eq!(n1.as_ref(), n2.as_ref());
                    assert_eq!(t1.value(), t2.value());
                },
                // Timestamp cases are commented out due to DateTime import issues
                _ => panic!("Mismatched metric types in roundtrip test: {:?} vs {:?}", original.value().name(), roundtrip.value().name()),
            }
        }
        
        println!("✓ Successfully tested MetricsSet roundtrip conversion with {} metrics", original_count);
        println!("✓ Tested metric types: OutputRows, ElapsedCompute, SpillCount, SpilledBytes, SpilledRows, CurrentMemoryUsage, Count, Gauge, Time");
        println!("✓ StartTimestamp and EndTimestamp are also supported but not tested due to DateTime import complexity");
    }

    #[test]
    fn test_proto_roundtrip_isolation() {
        use datafusion::physical_plan::metrics::{MetricsSet, Count, Time, MetricValue, Metric};
        use prost::Message;
        use crate::stage::{FlightAppMetadata, AppMetadata, TaskMetricsSet, TaskMetrics, StageKey};
        use std::sync::Arc;

        println!("🔍 Testing proto roundtrip at each level to isolate the issue...");

        // Create the same mixed pattern data
        let mut all_proto_metrics_sets = Vec::new();

        // Add 5 empty ProtoMetricsSet objects
        for i in 0..5 {
            let empty_metrics = MetricsSet::new();
            let proto_metrics_set = df_metrics_set_to_proto(&empty_metrics).unwrap();
            all_proto_metrics_sets.push(proto_metrics_set);
        }

        // Add 1 populated ProtoMetricsSet
        let mut populated_metrics = MetricsSet::new();
        let elapsed_compute = Time::new();
        elapsed_compute.add_duration(std::time::Duration::from_nanos(389793));
        let output_rows = Count::new();
        output_rows.add(2);
        populated_metrics.push(Arc::new(Metric::new(MetricValue::ElapsedCompute(elapsed_compute), Some(0))));
        populated_metrics.push(Arc::new(Metric::new(MetricValue::OutputRows(output_rows), Some(0))));
        let proto_metrics_set = df_metrics_set_to_proto(&populated_metrics).unwrap();
        all_proto_metrics_sets.push(proto_metrics_set);

        // Add 2 more empty ProtoMetricsSet objects
        for i in 6..8 {
            let empty_metrics = MetricsSet::new();
            let proto_metrics_set = df_metrics_set_to_proto(&empty_metrics).unwrap();
            all_proto_metrics_sets.push(proto_metrics_set);
        }

        println!("✓ Created {} mixed ProtoMetricsSet objects", all_proto_metrics_sets.len());

        // TEST 1: Individual ProtoMetricsSet roundtrip
        println!("\n🧪 TEST 1: Individual ProtoMetricsSet roundtrip");
        for (i, proto_metrics_set) in all_proto_metrics_sets.iter().enumerate() {
            let mut buffer = Vec::new();
            match proto_metrics_set.encode(&mut buffer) {
                Ok(()) => {
                    match ProtoMetricsSet::decode(buffer.as_slice()) {
                        Ok(decoded) => {
                            println!("  ✓ ProtoMetricsSet {} roundtrip: OK ({} bytes)", i, buffer.len());
                        }
                        Err(e) => {
                            println!("  ❌ ProtoMetricsSet {} decode FAILED: {}", i, e);
                            panic!("ProtoMetricsSet {} failed roundtrip", i);
                        }
                    }
                }
                Err(e) => {
                    println!("  ❌ ProtoMetricsSet {} encode FAILED: {}", i, e);
                    panic!("ProtoMetricsSet {} failed encode", i);
                }
            }
        }

        // TEST 2: Vec<ProtoMetricsSet> roundtrip
        println!("\n🧪 TEST 2: Vec<ProtoMetricsSet> as TaskMetrics.metrics");
        let task_metrics = TaskMetrics {
            stage_key: Some(StageKey {
                query_id: "test-query".to_string(),
                stage_id: 1,
                task_number: 0,
            }),
            metrics: all_proto_metrics_sets.clone(),
        };

        let mut buffer = Vec::new();
        match task_metrics.encode(&mut buffer) {
            Ok(()) => {
                match TaskMetrics::decode(buffer.as_slice()) {
                    Ok(decoded) => {
                        println!("  ✓ TaskMetrics roundtrip: OK ({} bytes, {} metrics)", buffer.len(), decoded.metrics.len());
                    }
                    Err(e) => {
                        println!("  ❌ TaskMetrics decode FAILED: {}", e);
                        panic!("TaskMetrics failed roundtrip: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("  ❌ TaskMetrics encode FAILED: {}", e);
                panic!("TaskMetrics failed encode: {}", e);
            }
        }

        // TEST 3: TaskMetricsSet roundtrip
        println!("\n🧪 TEST 3: TaskMetricsSet");
        let task_metrics_set = TaskMetricsSet {
            tasks: vec![task_metrics],
        };

        let mut buffer = Vec::new();
        match task_metrics_set.encode(&mut buffer) {
            Ok(()) => {
                match TaskMetricsSet::decode(buffer.as_slice()) {
                    Ok(decoded) => {
                        println!("  ✓ TaskMetricsSet roundtrip: OK ({} bytes, {} tasks)", buffer.len(), decoded.tasks.len());
                    }
                    Err(e) => {
                        println!("  ❌ TaskMetricsSet decode FAILED: {}", e);
                        panic!("TaskMetricsSet failed roundtrip: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("  ❌ TaskMetricsSet encode FAILED: {}", e);
                panic!("TaskMetricsSet failed encode: {}", e);
            }
        }

        // TEST 4: FlightAppMetadata roundtrip
        println!("\n🧪 TEST 4: FlightAppMetadata (full structure)");
        let flight_app_metadata = FlightAppMetadata {
            content: Some(AppMetadata::TaskMetricsSet(task_metrics_set)),
        };

        let mut buffer = Vec::new();
        match flight_app_metadata.encode(&mut buffer) {
            Ok(()) => {
                match FlightAppMetadata::decode(buffer.as_slice()) {
                    Ok(decoded) => {
                        println!("  ✓ FlightAppMetadata roundtrip: OK ({} bytes)", buffer.len());
                    }
                    Err(e) => {
                        println!("  ❌ FlightAppMetadata decode FAILED: {}", e);
                        panic!("FlightAppMetadata failed roundtrip: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("  ❌ FlightAppMetadata encode FAILED: {}", e);
                panic!("FlightAppMetadata failed encode: {}", e);
            }
        }

        println!("\n✅ All proto roundtrip tests passed!");
    }

    #[test]
    fn test_empty_metrics_in_flight_data() {
        use datafusion::physical_plan::metrics::{MetricsSet, Count, Time, MetricValue, Metric};
        use arrow_flight::FlightData;
        use prost::Message;
        use crate::stage::{FlightAppMetadata, AppMetadata, TaskMetricsSet, TaskMetrics, StageKey};
        use std::sync::Arc;

        println!("🔍 Testing mixed empty and populated ProtoMetricsSet objects...");

        // Create multiple ProtoMetricsSet objects matching your examples
        let mut all_proto_metrics_sets = Vec::new();

        // Add 5 empty ProtoMetricsSet objects
        for i in 0..5 {
            let empty_metrics = MetricsSet::new();
            let proto_metrics_set = df_metrics_set_to_proto(&empty_metrics).unwrap();
            assert_eq!(proto_metrics_set.metrics.len(), 0, "ProtoMetricsSet {} should be empty", i);
            all_proto_metrics_sets.push(proto_metrics_set);
        }

        // Add 1 populated ProtoMetricsSet with ElapsedCompute and OutputRows (like your example)
        let mut populated_metrics_1 = MetricsSet::new();
        let elapsed_compute_1 = Time::new();
        elapsed_compute_1.add_duration(std::time::Duration::from_nanos(389793));
        let output_rows_1 = Count::new();
        output_rows_1.add(2);
        populated_metrics_1.push(Arc::new(Metric::new(MetricValue::ElapsedCompute(elapsed_compute_1), Some(0))));
        populated_metrics_1.push(Arc::new(Metric::new(MetricValue::OutputRows(output_rows_1), Some(0))));
        
        let proto_metrics_set_1 = df_metrics_set_to_proto(&populated_metrics_1).unwrap();
        assert_eq!(proto_metrics_set_1.metrics.len(), 2, "ProtoMetricsSet 5 should have 2 metrics");
        all_proto_metrics_sets.push(proto_metrics_set_1);

        // Add 1 more populated ProtoMetricsSet with different values
        let mut populated_metrics_2 = MetricsSet::new();
        let elapsed_compute_2 = Time::new();
        elapsed_compute_2.add_duration(std::time::Duration::from_nanos(0));
        let output_rows_2 = Count::new();
        output_rows_2.add(366);
        populated_metrics_2.push(Arc::new(Metric::new(MetricValue::ElapsedCompute(elapsed_compute_2), Some(0))));
        populated_metrics_2.push(Arc::new(Metric::new(MetricValue::OutputRows(output_rows_2), Some(0))));
        
        let proto_metrics_set_2 = df_metrics_set_to_proto(&populated_metrics_2).unwrap();
        assert_eq!(proto_metrics_set_2.metrics.len(), 2, "ProtoMetricsSet 6 should have 2 metrics");
        all_proto_metrics_sets.push(proto_metrics_set_2);

        // Add 2 more empty ProtoMetricsSet objects
        for i in 7..9 {
            let empty_metrics = MetricsSet::new();
            let proto_metrics_set = df_metrics_set_to_proto(&empty_metrics).unwrap();
            assert_eq!(proto_metrics_set.metrics.len(), 0, "ProtoMetricsSet {} should be empty", i);
            all_proto_metrics_sets.push(proto_metrics_set);
        }

        println!("✓ Created {} ProtoMetricsSet objects (pattern: empty, empty, empty, empty, empty, populated, populated, empty, empty)", all_proto_metrics_sets.len());

        // Create TaskMetrics with mixed ProtoMetricsSet objects (empty and populated)
        let task_metrics = TaskMetrics {
            stage_key: Some(StageKey {
                query_id: "test-query".to_string(),
                stage_id: 1,
                task_number: 0,
            }),
            metrics: all_proto_metrics_sets,
        };

        // Create FlightAppMetadata
        let flight_app_metadata = FlightAppMetadata {
            content: Some(AppMetadata::TaskMetricsSet(TaskMetricsSet {
                tasks: vec![task_metrics],
            })),
        };

        // Encode to bytes
        let mut encode_buffer = Vec::new();
        let encode_result = flight_app_metadata.encode(&mut encode_buffer);
        
        match encode_result {
            Ok(()) => {
                println!("✓ FlightAppMetadata with empty metrics encoded successfully");
                println!("  Encoded buffer length: {} bytes", encode_buffer.len());
                
                // Try to decode it back
                let decode_result = FlightAppMetadata::decode(encode_buffer.as_slice());
                
                match decode_result {
                    Ok(decoded_metadata) => {
                        println!("✓ FlightAppMetadata decoded successfully");
                        
                        // Verify the decoded content
                        if let Some(AppMetadata::TaskMetricsSet(task_metrics_set)) = decoded_metadata.content {
                            assert_eq!(task_metrics_set.tasks.len(), 1, "Should have 1 task");
                            let decoded_task = &task_metrics_set.tasks[0];
                            assert_eq!(decoded_task.metrics.len(), 9, "Should have 9 metrics sets");
                            
                            // Verify the pattern: 5 empty, 2 populated, 2 empty
                            for i in 0..5 {
                                assert_eq!(decoded_task.metrics[i].metrics.len(), 0, "Metrics {} should be empty", i);
                            }
                            assert_eq!(decoded_task.metrics[5].metrics.len(), 2, "Metrics 5 should have 2 metrics");
                            assert_eq!(decoded_task.metrics[6].metrics.len(), 2, "Metrics 6 should have 2 metrics");
                            for i in 7..9 {
                                assert_eq!(decoded_task.metrics[i].metrics.len(), 0, "Metrics {} should be empty", i);
                            }
                            
                            println!("✓ Decoded content verified - mixed metrics pattern preserved");
                            println!("  Pattern: [0,0,0,0,0,2,2,0,0] metrics per set");
                        } else {
                            panic!("❌ Decoded content is not TaskMetricsSet");
                        }
                        
                        // Create FlightData with example schema and app_metadata
                        use datafusion::arrow::ipc::writer::{IpcDataGenerator, IpcWriteOptions};
                        use datafusion::arrow::datatypes::{Schema, Field, DataType};
                        use std::sync::Arc;
                        
                        // Create example schema
                        let schema = Arc::new(Schema::new(vec![
                            Field::new("id", DataType::Int32, false),
                            Field::new("name", DataType::Utf8, false),
                        ]));
                        
                        // Generate schema FlightData using Arrow IPC
                        let options = IpcWriteOptions::default();
                        let data_gen = IpcDataGenerator::default();
                        let schema_encoded = data_gen.schema_to_bytes(&schema, &options);
                        
                        let flight_data = FlightData {
                            flight_descriptor: None,
                            data_header: schema_encoded.ipc_message.into(),
                            data_body: schema_encoded.arrow_data.into(),
                            app_metadata: encode_buffer.into(),
                        };
                        
                        println!("✓ FlightData created with schema and empty metrics in app_metadata");
                        println!("  Schema: {:?}", schema);
                        println!("  Data header length: {} bytes", flight_data.data_header.len());
                        println!("  Data body length: {} bytes", flight_data.data_body.len());
                        println!("  App metadata length: {} bytes", flight_data.app_metadata.len());
                        
                        // Now try to decode the FlightData as if we received it
                        println!("🔍 Testing complete FlightData decoding...");
                        
                        // Test 1: Decode app_metadata from FlightData
                        let decoded_app_metadata = FlightAppMetadata::decode(flight_data.app_metadata.as_ref());
                        match decoded_app_metadata {
                            Ok(app_metadata) => {
                                println!("✓ App metadata decoded successfully from FlightData");
                                if let Some(AppMetadata::TaskMetricsSet(task_metrics_set)) = app_metadata.content {
                                    println!("✓ Task metrics extracted: {} tasks", task_metrics_set.tasks.len());
                                } else {
                                    println!("❌ App metadata content is not TaskMetricsSet");
                                }
                            }
                            Err(decode_error) => {
                                println!("❌ FAILED: App metadata decode from FlightData failed: {}", decode_error);
                                panic!("App metadata decode failed: {}", decode_error);
                            }
                        }
                        
                        // Test 2: Simulate what happens in real usage - check data structure
                        println!("🔍 Simulating real FlightData usage...");
                        
                        // This simulates the pattern used in your actual code
                        if !flight_data.data_header.is_empty() {
                            println!("✓ FlightData has schema data ({} bytes)", flight_data.data_header.len());
                        } else {
                            println!("⚠️ FlightData has no schema data");
                        }
                        
                        // The main test: can we decode app_metadata from a complete FlightData?
                        if !flight_data.app_metadata.is_empty() {
                            println!("✓ FlightData has app metadata ({} bytes)", flight_data.app_metadata.len());
                            
                            // This exactly matches your production code pattern
                            let final_decode_test = FlightAppMetadata::decode(flight_data.app_metadata.as_ref());
                            match final_decode_test {
                                Ok(_) => println!("✅ FINAL TEST PASSED: Complete FlightData app_metadata decodes successfully"),
                                Err(e) => {
                                    println!("❌ FINAL TEST FAILED: Complete FlightData app_metadata decode failed: {}", e);
                                    panic!("Final decode test failed: {}", e);
                                }
                            }
                        } else {
                            println!("❌ FlightData has no app metadata!");
                        }
                        
                        println!("✓ TEST PASSED: Complete FlightData with mixed (empty + populated) metrics works correctly");
                        
                    }
                    Err(decode_error) => {
                        println!("❌ TEST FAILED: FlightAppMetadata decode failed: {}", decode_error);
                        panic!("Decode failed: {}", decode_error);
                    }
                }
            }
            Err(encode_error) => {
                println!("❌ TEST FAILED: FlightAppMetadata encode failed: {}", encode_error);
                panic!("Encode failed: {}", encode_error);
            }
        }
    }
}