use std::pin::Pin;
use std::task::{Context, Poll};

use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::StageKey;
use crate::protobuf::{AppMetadata, FlightAppMetadata};
use arrow_flight::{FlightData, error::FlightError};
use dashmap::DashMap;
use futures::stream::Stream;
use pin_project::pin_project;
use prost::Message;
use std::sync::Arc;

/// MetricsCollectingStream wraps a FlightData stream and extracts metrics from app_metadata
/// while passing through all the other FlightData unchanged.
#[pin_project]
pub struct MetricsCollectingStream<S>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send,
{
    #[pin]
    inner: S,
    metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
}

impl<S> MetricsCollectingStream<S>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send,
{
    #[allow(dead_code)]
    pub fn new(
        stream: S,
        metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
    ) -> Self {
        Self {
            inner: stream,
            metrics_collection,
        }
    }

    fn extract_metrics_from_flight_data(
        metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
        flight_data: &mut FlightData,
    ) -> Result<(), FlightError> {
        if flight_data.app_metadata.is_empty() {
            return Ok(());
        }

        let metadata =
            FlightAppMetadata::decode(flight_data.app_metadata.as_ref()).map_err(|e| {
                FlightError::ProtocolError(format!("failed to decode app_metadata: {}", e))
            })?;

        let Some(content) = metadata.content else {
            return Err(FlightError::ProtocolError(
                "expected Some content in app_metadata".to_string(),
            ));
        };

        let AppMetadata::MetricsCollection(task_metrics_set) = content;

        for task_metrics in task_metrics_set.tasks {
            let Some(stage_key) = task_metrics.stage_key else {
                return Err(FlightError::ProtocolError(
                    "expected Some StageKey in MetricsCollectingStream, got None".to_string(),
                ));
            };
            metrics_collection.insert(stage_key, task_metrics.metrics);
        }

        flight_data.app_metadata.clear();
        Ok(())
    }
}

impl<S> Stream for MetricsCollectingStream<S>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send,
{
    type Item = Result<FlightData, FlightError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(mut flight_data))) => {
                // Extract metrics from app_metadata if present.
                match Self::extract_metrics_from_flight_data(
                    this.metrics_collection.clone(),
                    &mut flight_data,
                ) {
                    Ok(_) => Poll::Ready(Some(Ok(flight_data))),
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::{
        AppMetadata, FlightAppMetadata, MetricsCollection, StageKey, TaskMetrics,
    };
    use crate::test_utils::metrics::make_test_metrics_set_proto_from_seed;
    use arrow_flight::FlightData;
    use futures::stream::{self, StreamExt};
    use prost::{Message, bytes::Bytes};

    fn assert_protocol_error(result: Result<FlightData, FlightError>, expected_msg: &str) {
        let FlightError::ProtocolError(msg) = result.unwrap_err() else {
            panic!("expected FlightError::ProtocolError");
        };
        assert!(msg.contains(expected_msg));
    }

    fn make_flight_data(data: Vec<u8>, metadata: Option<FlightAppMetadata>) -> FlightData {
        let metadata_bytes = match metadata {
            Some(metadata) => metadata.encode_to_vec().into(),
            None => Bytes::new(),
        };
        FlightData {
            flight_descriptor: None,
            data_header: Bytes::new(),
            app_metadata: metadata_bytes,
            data_body: data.into(),
        }
    }

    // Tests that metrics are extracted from FlightData. Metrics are always stored per task, so this
    // tests creates some random metrics and tasks and puts them in the app_metadata field of
    // FlightData message. Then, it streams these messages through the MetricsCollectingStream
    // and asserts that the metrics are collected correctly.
    #[tokio::test]
    async fn test_metrics_collecting_stream_extracts_and_removes_metadata() {
        let stage_keys = vec![
            StageKey {
                query_id: "test_query".to_string(),
                stage_id: 1,
                task_number: 1,
            },
            StageKey {
                query_id: "test_query".to_string(),
                stage_id: 1,
                task_number: 2,
            },
        ];

        let app_metadatas = stage_keys
            .iter()
            .map(|stage_key| FlightAppMetadata {
                content: Some(AppMetadata::MetricsCollection(MetricsCollection {
                    tasks: vec![TaskMetrics {
                        stage_key: Some(stage_key.clone()),
                        // use the task number to seed the test metrics set for convenience
                        metrics: vec![make_test_metrics_set_proto_from_seed(stage_key.task_number)],
                    }],
                })),
            })
            .collect::<Vec<_>>();

        // Create test FlightData messages - some with metadata, some without
        let flight_messages = vec![
            make_flight_data(vec![1], Some(app_metadatas[0].clone())),
            make_flight_data(vec![2], None),
            make_flight_data(vec![3], Some(app_metadatas[1].clone())),
        ]
        .into_iter()
        .map(Ok);

        // Collect all messages from the stream. All should have empty app_metadata.
        let metrics_collection = Arc::new(DashMap::new());
        let input_stream = stream::iter(flight_messages);
        let collecting_stream =
            MetricsCollectingStream::new(input_stream, metrics_collection.clone());
        let collected_messages: Vec<FlightData> = collecting_stream
            .map(|result| result.unwrap())
            .collect()
            .await;

        // Assert the data is unchanged and app_metadata is cleared
        assert_eq!(collected_messages.len(), 3);
        assert!(
            collected_messages
                .iter()
                .all(|msg| msg.app_metadata.is_empty())
        );

        // Verify the data in the messages.
        assert_eq!(collected_messages[0].data_body, vec![1]);
        assert_eq!(collected_messages[1].data_body, vec![2]);
        assert_eq!(collected_messages[2].data_body, vec![3]);

        // Verify the correct metrics were collected
        assert_eq!(metrics_collection.len(), 2);
        for stage_key in stage_keys {
            let collected_metrics = metrics_collection.get(&stage_key).unwrap();
            assert_eq!(collected_metrics.len(), 1);
            assert_eq!(
                collected_metrics[0],
                make_test_metrics_set_proto_from_seed(stage_key.task_number)
            );
        }
    }

    #[tokio::test]
    async fn test_metrics_collecting_stream_error_missing_stage_key() {
        let task_metrics_with_no_stage_key = TaskMetrics {
            stage_key: None,
            metrics: vec![make_test_metrics_set_proto_from_seed(1)],
        };

        let invalid_app_metadata = FlightAppMetadata {
            content: Some(AppMetadata::MetricsCollection(MetricsCollection {
                tasks: vec![task_metrics_with_no_stage_key],
            })),
        };

        let invalid_flight_data = make_flight_data(vec![1], Some(invalid_app_metadata));

        let error_stream = stream::iter(vec![Ok(invalid_flight_data)]);
        let mut collecting_stream =
            MetricsCollectingStream::new(error_stream, Arc::new(DashMap::new()));

        let result = collecting_stream.next().await.unwrap();
        assert_protocol_error(
            result,
            "expected Some StageKey in MetricsCollectingStream, got None",
        );
    }

    #[tokio::test]
    async fn test_metrics_collecting_stream_error_decoding_metadata() {
        let flight_data_with_invalid_metadata = FlightData {
            flight_descriptor: None,
            data_header: Bytes::new(),
            app_metadata: vec![0xFF, 0xFF, 0xFF, 0xFF].into(), // Invalid protobuf data
            data_body: vec![4, 5, 6].into(),
        };

        let error_stream = stream::iter(vec![Ok(flight_data_with_invalid_metadata)]);
        let mut collecting_stream =
            MetricsCollectingStream::new(error_stream, Arc::new(DashMap::new()));

        let result = collecting_stream.next().await.unwrap();
        assert_protocol_error(result, "failed to decode app_metadata");
    }

    #[tokio::test]
    async fn test_metrics_collecting_stream_error_propagation() {
        let metrics_collection = Arc::new(DashMap::new());

        // Create a stream that emits an error - should be propagated through
        let stream_error = FlightError::ProtocolError("stream error from inner stream".to_string());
        let error_stream = stream::iter(vec![Err(stream_error)]);
        let mut collecting_stream = MetricsCollectingStream::new(error_stream, metrics_collection);

        let result = collecting_stream.next().await.unwrap();
        assert_protocol_error(result, "stream error from inner stream");
    }
}
