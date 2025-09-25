use arrow_flight::{FlightData, error::FlightError};
use futures::stream::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::pin;

/// TrailingFlightDataStream - wraps a FlightData stream. It calls the `on_complete` closure when the stream is finished.
/// If the closure returns a new stream, it will be appended to the original stream and consumed.
#[pin_project]
pub struct TrailingFlightDataStream<S, F>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send,
    F: FnOnce() -> Result<Option<S>, FlightError>,
{
    #[pin]
    inner: S,
    on_complete: Option<F>,
    #[pin]
    trailing_stream: Option<S>,
}

impl<S, F> TrailingFlightDataStream<S, F>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send,
    F: FnOnce() -> Result<Option<S>, FlightError>,
{
    // TODO: remove
    #[allow(dead_code)]
    pub fn new(on_complete: F, inner: S) -> Self {
        Self {
            inner,
            on_complete: Some(on_complete),
            trailing_stream: None,
        }
    }
}

impl<S, F> Stream for TrailingFlightDataStream<S, F>
where
    S: Stream<Item = Result<FlightData, FlightError>> + Send,
    F: FnOnce() -> Result<Option<S>, FlightError>,
{
    type Item = Result<FlightData, FlightError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut().project();

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Ok(flight_data))) => Poll::Ready(Some(Ok(flight_data))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => {
                if let Some(trailing_stream) = this.trailing_stream.as_mut().as_pin_mut() {
                    return trailing_stream.poll_next(cx);
                }
                if let Some(on_complete) = this.on_complete.take() {
                    if let Some(trailing_stream) = on_complete()? {
                        this.trailing_stream.set(Some(trailing_stream));
                        return self.poll_next(cx);
                    }
                }
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow_flight::FlightData;
    use arrow_flight::decode::FlightRecordBatchStream;
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use futures::stream::{self, StreamExt};
    use std::sync::Arc;

    fn create_trailing_flight_data_stream(
        name_array: StringArray,
        value_array: Int32Array,
    ) -> Pin<Box<dyn Stream<Item = Result<FlightData, FlightError>> + Send>> {
        create_flight_data_stream_inner(name_array, value_array, true)
    }

    fn create_flight_data_stream(
        name_array: StringArray,
        value_array: Int32Array,
    ) -> Pin<Box<dyn Stream<Item = Result<FlightData, FlightError>> + Send>> {
        create_flight_data_stream_inner(name_array, value_array, false)
    }

    // Creates a stream of RecordBatches.
    fn create_flight_data_stream_inner(
        name_array: StringArray,
        value_array: Int32Array,
        is_trailing: bool,
    ) -> Pin<Box<dyn Stream<Item = Result<FlightData, FlightError>> + Send>> {
        assert_eq!(
            name_array.len(),
            value_array.len(),
            "StringArray and Int32Array must have equal lengths"
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let batches: Vec<RecordBatch> = (0..name_array.len())
            .map(|i| {
                let name_slice = name_array.slice(i, 1);
                let value_slice = value_array.slice(i, 1);

                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(name_slice), Arc::new(value_slice)],
                )
                .unwrap()
            })
            .collect();

        let batch_stream = futures::stream::iter(batches.into_iter().map(Ok));
        let flight_stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(batch_stream);

        // By default, this encoder will emit a schema message as the first message in the stream.
        // Since we are concatenating streams, we need to drop the schema message from the trailing stream.
        if is_trailing {
            // Skip the schema message
            return Box::pin(flight_stream.skip(1));
        }
        Box::pin(flight_stream)
    }

    #[tokio::test]
    async fn test_basic_streaming_functionality() {
        let name_array = StringArray::from(vec!["a", "b", "c"]);
        let value_array = Int32Array::from(vec![1, 2, 3]);
        let inner_stream = create_flight_data_stream(name_array, value_array);

        let name_array = StringArray::from(vec!["d", "e", "f"]);
        let value_array = Int32Array::from(vec![5, 6, 7]);
        let trailing_stream = create_trailing_flight_data_stream(name_array, value_array);

        let on_complete = || Ok(Some(trailing_stream));

        let trailing_stream = TrailingFlightDataStream::new(on_complete, inner_stream);
        let record_batches = FlightRecordBatchStream::new_from_flight_data(trailing_stream)
            .collect::<Vec<Result<RecordBatch, FlightError>>>()
            .await;

        assert_eq!(record_batches.len(), 6);
        assert!(record_batches.iter().all(|batch| batch.is_ok()));
        assert_eq!(
            record_batches
                .iter()
                .map(|batch| batch
                    .as_ref()
                    .unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0))
                .collect::<Vec<_>>(),
            vec!["a", "b", "c", "d", "e", "f"]
        );
    }

    #[tokio::test]
    async fn test_error_handling_in_inner_stream() {
        let mut stream =
            create_flight_data_stream(StringArray::from(vec!["item1"]), Int32Array::from(vec![1]));
        let schema_message = stream.next().await.unwrap().unwrap();
        let flight_data = stream.next().await.unwrap().unwrap();
        let data = vec![
            Ok(schema_message),
            Ok(flight_data),
            Err(FlightError::ExternalError(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "test error",
            )))),
        ];
        let inner_stream = stream::iter(data);
        let on_complete = || Ok(None);
        let trailing_stream = TrailingFlightDataStream::new(on_complete, inner_stream);
        let record_batches = FlightRecordBatchStream::new_from_flight_data(trailing_stream)
            .collect::<Vec<Result<RecordBatch, FlightError>>>()
            .await;

        assert_eq!(record_batches.len(), 2);
        assert!(record_batches[0].is_ok());
        assert!(record_batches[1].is_err());
    }

    #[tokio::test]
    async fn test_error_handling_in_on_complete_callback() {
        let name_array = StringArray::from(vec!["item1"]);
        let value_array = Int32Array::from(vec![1]);
        let inner_stream = create_flight_data_stream(name_array, value_array);

        let on_complete = || -> Result<Option<_>, FlightError> {
            Err(FlightError::ExternalError(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "callback error",
            ))))
        };

        let trailing_stream = TrailingFlightDataStream::new(on_complete, inner_stream);
        let record_batches = FlightRecordBatchStream::new_from_flight_data(trailing_stream)
            .collect::<Vec<Result<RecordBatch, FlightError>>>()
            .await;
        assert_eq!(record_batches.len(), 2);
        assert!(record_batches[0].is_ok());
        assert!(record_batches[1].is_err());
    }

    #[tokio::test]
    async fn test_stream_with_no_trailer() {
        let inner_stream = create_flight_data_stream(
            StringArray::from(vec!["item1"] as Vec<&str>),
            Int32Array::from(vec![1] as Vec<i32>),
        );
        let on_complete = || Ok(None);
        let trailing_stream = TrailingFlightDataStream::new(on_complete, inner_stream);
        let record_batches = FlightRecordBatchStream::new_from_flight_data(trailing_stream)
            .collect::<Vec<Result<RecordBatch, FlightError>>>()
            .await;
        assert_eq!(record_batches.len(), 1);
        assert!(record_batches[0].is_ok());
    }
}
