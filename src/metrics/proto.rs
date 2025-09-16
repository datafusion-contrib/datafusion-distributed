use datafusion::physical_plan::metrics::{Metric, MetricValue, MetricsSet};
use datafusion::error::DataFusionError;
use std::sync::Arc;

/// A ProtoMetric mirrors `datafusion::physical_plan::metrics::Metric`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ProtoMetric {
    #[prost(oneof="ProtoMetricValue", tags="1")]
    // This field is *always* set. It is marked optional due to protobuf "oneof" requirements.
    pub metric: Option<ProtoMetricValue>,
    #[prost(message, repeated, tag="2")]
    pub labels: Vec<ProtoLabel>,
    #[prost(uint64, optional, tag="3")]
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
    // TODO: implement all the other types
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
    Ok(ProtoMetricsSet { metrics })
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
        _ => Err(DataFusionError::Internal(format!("unsupported proto metric type: {}", metric.value().name()))),
    }
}

/// proto_metric_to_df converts a `ProtoMetric` to a `datafusion::physical_plan::metrics::Metric`. It consumes the ProtoMetric.
pub fn proto_metric_to_df(metric: ProtoMetric) -> Result<Arc<Metric>, DataFusionError> {
    use datafusion::physical_plan::metrics::{Count, Time, Label};
    
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
        None => Err(DataFusionError::Internal("proto metric is missing the metric field".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::{Count, Time, MetricValue, Metric, Label};

    #[test]
    fn test_metric_roundtrip() {
        use datafusion::physical_plan::metrics::MetricsSet;
        
        let count = Count::new();
        count.add(1234);
        let time = Time::new();
        time.add_duration(std::time::Duration::from_millis(100));
        
        // Create a MetricsSet with multiple metrics
        let mut metrics_set = MetricsSet::new();
        metrics_set.push(Arc::new(Metric::new(MetricValue::OutputRows(count), Some(0))));
        metrics_set.push(Arc::new(Metric::new(MetricValue::ElapsedCompute(time), Some(1))));
        
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
                // TODO: implement all the other types
                _ => panic!("Unsupported metric type in roundtrip test"),
            }
        }
        
        println!("✓ Successfully tested MetricsSet roundtrip conversion with {} metrics", original_count);
    }
}