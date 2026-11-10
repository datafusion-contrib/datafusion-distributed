use prost::Message;
use std::any::Any;

/// A [`WorkUnit`] is a single unit of runtime metadata produced by a
/// [`crate::WorkUnitFeedProvider`] and consumed by a leaf [`datafusion::physical_plan::ExecutionPlan`]
/// via an embedded [`crate::WorkUnitFeed`].
///
/// It can be anything:
/// - A Parquet file address in S3 that must be read.
/// - An HTTP query that should be issued to an external API.
/// - An external database query that should be executed externally.
/// - Etc...
///
/// Any protobuf message that implements `prost`'s [`Message`] trait is automatically a
/// valid `WorkUnit` — no explicit `impl` block is required. Work units are serialized
/// with `prost` when streamed from the coordinator to workers, and decoded back to the
/// concrete `Self::WorkUnit` type of the receiving [`crate::WorkUnitFeedProvider`].
///
/// # Example
///
/// ```rust
/// # use datafusion_distributed::WorkUnit;
///
/// #[derive(Clone, PartialEq, ::prost::Message)]
/// struct FileAddressWorkUnit {
///     #[prost(string, tag = "1")]
///     url: String,
/// }
///
/// let file_address = FileAddressWorkUnit {
///     url: "s3://my-bucket/file.format".into()
/// };
///
/// let work_unit = Box::new(file_address) as Box<dyn WorkUnit>;
/// ```
pub trait WorkUnit: Message {
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    fn encode_to_bytes(&self) -> Vec<u8>;
}

impl<T: Message + Default + 'static> WorkUnit for T {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
    fn encode_to_bytes(&self) -> Vec<u8> {
        self.encode_to_vec()
    }
}
