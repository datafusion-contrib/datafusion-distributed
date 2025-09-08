use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_flight::{FlightData, error::FlightError};
use futures::stream::Stream;
use prost::Message;
use std::sync::Arc;
use crate::stage::TaskMetricsSet;
use crate::metrics::proto::ProtoMetricsSet;
use dashmap::DashMap;
use crate::stage::StageKey;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightAppMetadata {
    #[prost(oneof = "AppMetadata", tags = "1")]
    pub content: Option<AppMetadata>,
}

#[derive(Clone, PartialEq, ::prost::Oneof)]
pub enum AppMetadata {
    #[prost(message, tag="1")]
    TaskMetricsSet(TaskMetricsSet),
}

/// MetricsCollectingStream - wraps a FlightData stream and extracts metrics from app_metadata
/// while passing through all FlightData unchanged
pub struct MetricsCollectingStream<S> 
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
{
    inner: S,
    collected_metrics: Arc<DashMap<StageKey, Vec<ProtoMetricsSet>>>,
}

impl<S> MetricsCollectingStream<S>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
{
    pub fn new(stream: S) -> Self {
        Self {
            inner: stream,
            collected_metrics: Arc::new(DashMap::new()),
        }
    }

    /// Get a handle to the collected metrics
    pub fn metrics_handle(&self) -> Arc<DashMap<StageKey, Vec<ProtoMetricsSet>>> {
        Arc::clone(&self.collected_metrics)
    }

    fn extract_metrics_from_flight_data(&self, flight_data: &FlightData) {
        if !flight_data.app_metadata.is_empty() {
            if let Ok(metadata) = FlightAppMetadata::decode(flight_data.app_metadata.as_ref()) {
                if let Some(AppMetadata::TaskMetricsSet(task_metrics_set)) = metadata.content {
                    for task_metrics in task_metrics_set.tasks {
                        if let Some(stage_key) = task_metrics.stage_key {
                            self.collected_metrics.insert(stage_key, task_metrics.metrics);
                        }
                    }
                }
           }
        }
    }
}

impl<S> Stream for MetricsCollectingStream<S>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
{
    type Item = Result<FlightData, FlightError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(flight_data))) => {
                // Extract metrics from app_metadata if present
                self.extract_metrics_from_flight_data(&flight_data);
                
                // Pass through the FlightData unchanged
                Poll::Ready(Some(Ok(flight_data)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Wrap a FlightData stream with metrics collection capability
pub fn new_metrics_collecting_stream<S>(
    stream: S
) -> (MetricsCollectingStream<S>, Arc<DashMap<StageKey, Vec<ProtoMetricsSet>>>)
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin + 'static,
{
    let metrics_stream = MetricsCollectingStream::new(stream);
    let metrics_handle = metrics_stream.metrics_handle();
    (metrics_stream, metrics_handle)
}