use crate::DefaultSessionBuilder;
use crate::common::ttl_map::{TTLMap, TTLMapConfig};
use crate::flight_service::WorkerSessionBuilder;
use crate::flight_service::do_get::TaskData;
use crate::protobuf::StageKey;
use crate::protobuf::observability::observability_service_server::ObservabilityService;
use crate::protobuf::observability::{PingRequest, PingResponse};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::ExecutionPlan;
use futures::stream::BoxStream;
use std::sync::Arc;
use tokio::sync::OnceCell;
use tonic::{Request, Response, Status, Streaming};

#[allow(clippy::type_complexity)]
#[derive(Default)]
pub(super) struct WorkerHooks {
    pub(super) on_plan:
        Vec<Arc<dyn Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> + Sync + Send>>,
}

pub struct Worker {
    pub(super) runtime: Arc<RuntimeEnv>,
    pub(super) task_data_entries: Arc<TTLMap<StageKey, Arc<OnceCell<TaskData>>>>,
    pub(super) session_builder: Arc<dyn WorkerSessionBuilder + Send + Sync>,
    pub(super) hooks: WorkerHooks,
    pub(super) max_message_size: Option<usize>,
}

/// A wrapper for [Arc<Worker>] useful for implementing multiple services
/// such as [with_flight_service()] and [with_observability_service()].
#[derive(Clone)]
pub struct SharedWorker(Arc<Worker>);

impl SharedWorker {
    pub fn new(worker: Worker) -> Self {
        Self(Arc::new(worker))
    }
}

impl Default for Worker {
    fn default() -> Self {
        let ttl_map = TTLMap::try_new(TTLMapConfig::default())
            .expect("Instantiating a TTLMap with default params should never fail");
        Self {
            runtime: Arc::new(RuntimeEnv::default()),
            task_data_entries: Arc::new(ttl_map),
            session_builder: Arc::new(DefaultSessionBuilder),
            hooks: WorkerHooks::default(),
            max_message_size: Some(usize::MAX),
        }
    }
}

impl Worker {
    /// Builds a [Worker] with a custom [WorkerSessionBuilder]. Use this
    /// method whenever you need to add custom stuff to the `SessionContext` that executes the query.
    pub fn from_session_builder(
        session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    ) -> Self {
        Self {
            session_builder: Arc::new(session_builder),
            ..Default::default()
        }
    }

    /// Sets a [RuntimeEnv] to be used in all the queries this [Worker] will handle during
    /// its lifetime.
    pub fn with_runtime_env(mut self, runtime_env: Arc<RuntimeEnv>) -> Self {
        self.runtime = runtime_env;
        self
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

    /// Converts this [Worker] into a [`FlightServiceServer`] with high default message size limits.
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
    /// ```
    /// # use datafusion_distributed::Worker;
    /// # use tonic::transport::Server;
    /// # use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    /// # async fn f() {
    ///
    /// let worker = Worker::default();
    /// let server = worker.into_flight_server();
    ///
    /// Server::builder()
    ///     .add_service(Worker::default().into_flight_server())
    ///     .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080))
    ///     .await;
    ///
    /// # }
    /// ```
    pub fn into_flight_server(self) -> FlightServiceServer<Self> {
        FlightServiceServer::new(self)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX)
    }
}

#[async_trait]
impl FlightService for Worker {
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

#[async_trait]
impl FlightService for SharedWorker {
    type HandshakeStream = <Worker as FlightService>::HandshakeStream;
    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        FlightService::handshake(self.0.as_ref(), request).await
    }

    type ListFlightsStream = <Worker as FlightService>::ListFlightsStream;

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        FlightService::list_flights(self.0.as_ref(), request).await
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        FlightService::get_flight_info(self.0.as_ref(), request).await
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        FlightService::poll_flight_info(self.0.as_ref(), request).await
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        FlightService::get_schema(self.0.as_ref(), request).await
    }

    type DoGetStream = <Worker as FlightService>::DoGetStream;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        FlightService::do_get(self.0.as_ref(), request).await
    }

    type DoPutStream = <Worker as FlightService>::DoPutStream;

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        FlightService::do_put(self.0.as_ref(), request).await
    }

    type DoExchangeStream = <Worker as FlightService>::DoExchangeStream;

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        FlightService::do_exchange(self.0.as_ref(), request).await
    }

    type DoActionStream = <Worker as FlightService>::DoActionStream;

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        FlightService::do_action(self.0.as_ref(), request).await
    }

    type ListActionsStream = <Worker as FlightService>::ListActionsStream;

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        FlightService::list_actions(self.0.as_ref(), request).await
    }
}

#[tonic::async_trait]
impl ObservabilityService for Worker {
    async fn ping(
        &self,
        _request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingResponse>, tonic::Status> {
        Ok(tonic::Response::new(PingResponse { value: 1 }))
    }
}

#[tonic::async_trait]
impl ObservabilityService for SharedWorker {
    async fn ping(
        &self,
        request: tonic::Request<PingRequest>,
    ) -> Result<tonic::Response<PingResponse>, tonic::Status> {
        ObservabilityService::ping(self.0.as_ref(), request).await
    }
}
