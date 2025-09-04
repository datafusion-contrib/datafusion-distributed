use datafusion::physical_plan::metrics::{Metric, MetricValue};
use datafusion::error::DataFusionError;

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

/// df_metric_to_proto converts a `datafusion::physical_plan::metrics::Metric` to a `ProtoMetric`.
pub fn df_metric_to_proto(metric: &Metric) -> Result<ProtoMetric, DataFusionError> {
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

/// proto_metric_to_df converts a `ProtoMetric` to a `datafusion::physical_plan::metrics::Metric`.
pub fn proto_metric_to_df(metric: ProtoMetric) -> Result<Metric, DataFusionError> {
    use datafusion::physical_plan::metrics::{Count, Time, Label};
    
    let partition = metric.partition.map(|p| p as usize);
    let labels = metric.labels.into_iter().map(|proto_label| {
        Label::new(proto_label.name, proto_label.value)
    }).collect();
    
    match metric.metric {
        Some(ProtoMetricValue::OutputRows(rows)) => {
            let count = Count::new();
            count.add(rows.value as usize);
            Ok(Metric::new_with_labels(MetricValue::OutputRows(count), partition, labels))
        },
        Some(ProtoMetricValue::ElapsedCompute(elapsed)) => {
            let time = Time::new();
            time.add_duration(std::time::Duration::from_nanos(elapsed.value));
            Ok(Metric::new_with_labels(MetricValue::ElapsedCompute(time), partition, labels))
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
        let count = Count::new();
        count.add(1234);
        let time = Time::new();
        time.add_duration(std::time::Duration::from_millis(100));
        
        let metrics = vec![
            Metric::new(MetricValue::OutputRows(count), Some(0)),
            Metric::new(MetricValue::ElapsedCompute(time), Some(1)),
            // TODO: implement all the other types
        ];
        
        for metric in metrics {
            let proto = df_metric_to_proto(&metric).unwrap();
            let roundtrip = proto_metric_to_df(proto).unwrap();
            
            assert_eq!(metric.partition(), roundtrip.partition());
            assert_eq!(metric.labels().len(), roundtrip.labels().len());
            
            match (metric.value(), roundtrip.value()) {
                (MetricValue::OutputRows(orig), MetricValue::OutputRows(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                },
                (MetricValue::ElapsedCompute(orig), MetricValue::ElapsedCompute(rt)) => {
                    assert_eq!(orig.value(), rt.value());
                },
                // TODO: implement all the other types
                _ => unimplemented!(),
            }
        }
    }
}