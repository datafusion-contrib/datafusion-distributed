use arrow_flight::{FlightData, error::FlightError};
use futures::stream::Stream;
use prost::Message;
use std::sync::Arc;
use dashmap::DashMap;
use crate::stage::{StageKey, FlightAppMetadata, AppMetadata, TaskMetrics, TaskMetricsSet};
use crate::metrics::proto::{df_metrics_set_to_proto, ProtoMetricsSet};
use std::pin::Pin;
use std::task::{Context, Poll};
use super::do_get::TaskData;
use crate::stage::MetricsCollector;
use arrow::datatypes::SchemaRef;
use crate::stage::ExecutionStage;
use datafusion::physical_plan::metrics::MetricsSet;

/// MetricsEmittingStream - wraps a FlightData stream. It uses the provided partition and task data
/// to determine if the task is done. If so, it emits an empty FlightData message with the metrics
/// in the app_metadata.
/// 
pub struct MetricsEmittingStream<S, F> 
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
    F: Fn() -> bool + Unpin,
{
    inner: S,
    stage_key: StageKey,
    stage: Arc<ExecutionStage>,
    // Used to make an empty FlightData message with the schema.
    empty_flight_data: FlightData,
    /// on_complete is called when the stream is finished. We expect it to return true if the task is finished.
    /// If this stream finished the task, then it will emit metrics. Otherwise, it will not. 
    on_complete: F,
    /// is_finished is set to true when this stream is finished. We use it to check that we only emit metrics once.
    is_finished: bool,
}

impl<S, F> MetricsEmittingStream<S, F>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
    F: Fn() -> bool + Unpin,
     {
    pub fn new(stage_key: StageKey, stage: Arc<ExecutionStage>, empty_flight_data: FlightData, on_complete: F, inner: S) -> Self {
        
        Self {
            inner: inner,
            stage_key,
            stage,
            empty_flight_data,
            on_complete,
            is_finished: false,
        }
    }
}

impl<S, F> MetricsEmittingStream<S, F>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
    F: Fn() -> bool + Unpin,
     {
    fn create_metrics_flight_data(&self) -> Result<FlightData, FlightError> {
        // Get the metrics for the task executed on this worker. Separately, collect metrics for child tasks.
        let (task_metrics, mut child_task_metrics) = MetricsCollector::new().collect(&self.stage).map_err(|err| FlightError::ProtocolError(err.to_string()))?;

        // Add the metrics for this task into the collection of task metrics. 
        // Skip any metrics that can't be converted to proto (unsupported types)
        let proto_task_metrics = task_metrics.iter()
            .map(|metrics| df_metrics_set_to_proto(metrics).unwrap_or(ProtoMetricsSet::default()))
            .collect::<Vec<_>>(); 
        child_task_metrics.insert(self.stage_key.clone(), proto_task_metrics.clone());

        // Serialize the metrics for all tasks.
        let mut task_metrics_set = vec![];
        for (stage_key, metrics) in child_task_metrics.into_iter() {
            task_metrics_set.push(TaskMetrics {
                stage_key: Some(stage_key),
                metrics,
            });
        } 
            
        let flight_app_metadata = FlightAppMetadata {
            content: Some(AppMetadata::TaskMetricsSet(TaskMetricsSet {
                tasks: task_metrics_set,
            })),
        };
        let mut buf = Vec::new();
        flight_app_metadata.encode(&mut buf).map_err(|err| FlightError::ProtocolError(err.to_string()))?;
        
        // Return a FlightData containing metrics only.
        Ok(FlightData::with_app_metadata(self.empty_flight_data.clone(), buf))
    }
}

impl<S, F> Stream for MetricsEmittingStream<S, F> 
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send + Unpin,
    F: Fn() -> bool + Unpin,
    {
    type Item = Result<FlightData, FlightError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Since everything is Unpin, we can get mutable access.
        let this = Pin::get_mut(self.as_mut());

       // poll_next requires a Pin<&mut S>, so we need to wrap it in a Pin. 
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(flight_data))) => {
                Poll::Ready(Some(Ok(flight_data)))
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => {
                if !this.is_finished {
                    // Check if we should emit metrics.
                    this.is_finished = true;
                    if (this.on_complete)() {
                        return Poll::Ready(Some(Ok(this.create_metrics_flight_data()?)));
                    };
                } 
                Poll::Ready(None)
            },
            Poll::Pending => Poll::Pending,
        }
    }
}