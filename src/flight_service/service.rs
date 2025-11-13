use crate::common::ttl_map::{TTLMap, TTLMapConfig};
use crate::flight_service::DistributedSessionBuilder;
use crate::flight_service::do_get::TaskData;
use crate::protobuf::StageKey;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::BoxStream;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status, Streaming};

#[allow(clippy::type_complexity)]
#[derive(Default)]
/// Callbacks for the ArrowFlightEndpoint which allow users to modify / observe
/// [ArrowFlightEndpoint] internals.
pub(super) struct ArrowFlightEndpointHooks {
    pub(super) on_plan:
        Vec<Arc<dyn Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> + Sync + Send>>,
    pub(super) after_plan:
        Vec<Arc<dyn Fn(Arc<dyn ExecutionPlan>) + Send + Sync>>,
}

/// Endpoint for distributed datafusion task plans. 
pub struct ArrowFlightEndpoint {
    pub(super) runtime: Arc<RuntimeEnv>,
    pub(super) task_data_entries: Arc<TTLMap<StageKey, Arc<OnceCell<TaskData>>>>,
    pub(super) session_builder: Arc<dyn DistributedSessionBuilder + Send + Sync>,
    pub(super) hooks: ArrowFlightEndpointHooks,
    pub(super) max_message_size: Option<usize>,
}

impl ArrowFlightEndpoint {
    pub fn try_new(
        session_builder: impl DistributedSessionBuilder + Send + Sync + 'static,
    ) -> Result<Self, DataFusionError> {
        let ttl_map = TTLMap::try_new(TTLMapConfig::default())?;
        Ok(Self {
            runtime: Arc::new(RuntimeEnv::default()),
            task_data_entries: Arc::new(ttl_map),
            session_builder: Arc::new(session_builder),
            hooks: ArrowFlightEndpointHooks::default(),
            max_message_size: Some(usize::MAX),
        })
    }

    /// Adds a callback for when an [ExecutionPlan] is received in the `do_get` call.
    ///
    /// The callback takes the plan and returns another plan that must be either the same,
    /// or equivalent in terms of execution. Mutating the plan by adding nodes or removing them
    /// will make the query blow up in unexpected ways.
    pub fn add_on_plan_hook(
        &mut self,
        hook: impl Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> + Sync + Send + 'static,
    ) {
        self.hooks.on_plan.push(Arc::new(hook));
    }

    /// Adds a callback for when an [ExecutionPlan] is finished executing all partitions in the
    /// `do_get` call. These block the consumer and are executed exactly once.
    pub fn add_after_plan_hook(
        &mut self,
        hook: Arc<dyn Fn(Arc<dyn ExecutionPlan>) + Send + Sync>,
    ) {
        self.hooks.after_plan.push(hook);
    }

    /// Set the maximum message size for FlightData chunks.
    ///
    /// Defaults to `usize::MAX` to minimize chunking overhead for internal communication.
    /// See [`FlightDataEncoderBuilder::with_max_flight_data_size`] for details.
    ///
    /// If you change this to a lower value, ensure you configure the server's
    /// max_encoding_message_size and max_decoding_message_size to at least 2x this value
    /// to allow for overhead. For most use cases, the default of `usize::MAX` is appropriate.
    ///
    /// [`FlightDataEncoderBuilder::with_max_flight_data_size`]: https://arrow.apache.org/rust/arrow_flight/encode/struct.FlightDataEncoderBuilder.html#structfield.max_flight_data_size
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = Some(size);
        self
    }

    /// Converts this endpoint into a [`FlightServiceServer`] with high default message size limits.
    ///
    /// This is a convenience method that wraps the endpoint in a [`FlightServiceServer`] and
    /// configures it with `max_decoding_message_size(usize::MAX)` and
    /// `max_encoding_message_size(usize::MAX)` to avoid message size limitations for internal
    /// communication.
    ///
    /// You can further customize the returned server by chaining additional tonic methods.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let endpoint = ArrowFlightEndpoint::try_new(session_builder)?;
    /// let server = endpoint.into_flight_server();
    /// // Can chain additional tonic methods if needed
    /// // let server = server.some_other_tonic_method(...);
    /// ```
    pub fn into_flight_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX)
    }
}

#[async_trait]
impl FlightService for ArrowFlightEndpoint {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.get(request).await
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}
