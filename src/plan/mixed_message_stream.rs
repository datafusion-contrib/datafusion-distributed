use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_flight::{FlightData, error::FlightError};
use futures::stream::Stream;
use std::sync::Arc;
use crate::metrics::proto::ProtoMetricsSet;
use dashmap::DashMap;
use crate::stage::StageKey;
use crate::stage::{FlightAppMetadata, AppMetadata};
use prost::Message;

/// MetricsCollectingStream - wraps a FlightData stream and extracts metrics from app_metadata
/// while passing through all FlightData unchanged
pub struct MetricsCollectingStream<S> 
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
{
    inner: S,
    metrics_collection: Arc<DashMap<StageKey, Vec<ProtoMetricsSet>>>,
}

impl<S> MetricsCollectingStream<S>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
{
    pub fn new(stream: S, metrics_collection: Arc<DashMap<StageKey, Vec<ProtoMetricsSet>>>) -> Self {
        Self {
            inner: stream,
            metrics_collection,
        }
    }

    /// return true if we extracted metrics from the app_metadata.
    fn extract_metrics_from_flight_data(&self, flight_data: &mut FlightData) -> Result<(), FlightError> {
        if !flight_data.app_metadata.is_empty() {
            return match FlightAppMetadata::decode(flight_data.app_metadata.as_ref()) {
                Ok(metadata) => {
                    if let Some(AppMetadata::TaskMetricsSet(task_metrics_set)) = metadata.content {
                        for task_metrics in task_metrics_set.tasks {
                            if let Some(stage_key) = task_metrics.stage_key {
                                self.metrics_collection.insert(stage_key.clone(), task_metrics.metrics.clone());
                                println!("CLIENT {} {:?}", stage_key, task_metrics.metrics);
                            }
                        }
                    }
                    flight_data.app_metadata.clear();
                    Ok(())
                }
                Err(e) => {
                    Err(FlightError::ProtocolError(format!("failed to decode app_metadata: {}", e)))
                }
           }
        }
        Ok(())
    }
}

impl<S> Stream for MetricsCollectingStream<S>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
{
    type Item = Result<FlightData, FlightError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(mut flight_data))) => {
                // Extract metrics from app_metadata if present. 
                match self.extract_metrics_from_flight_data(&mut flight_data) {
                    Ok(_) => {
                        Poll::Ready(Some(Ok(flight_data)))
                    }
                    Err(e) => {
                         Poll::Ready(Some(Err(e)))
                    }
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
