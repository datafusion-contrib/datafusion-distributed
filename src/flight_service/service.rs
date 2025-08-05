use crate::channel_manager::ChannelManager;
use crate::flight_service::session_builder::NoopSessionBuilder;
use crate::flight_service::stream_partitioner_registry::StreamPartitionerRegistry;
use crate::flight_service::SessionBuilder;
use crate::ChannelResolver;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::stream::BoxStream;
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

pub struct ArrowFlightEndpoint {
    pub(super) channel_manager: Arc<ChannelManager>,
    pub(super) runtime: Arc<RuntimeEnv>,
    pub(super) partitioner_registry: Arc<StreamPartitionerRegistry>,
    pub(super) session_builder: Arc<dyn SessionBuilder + Send + Sync>,
}

impl ArrowFlightEndpoint {
    pub fn new(channel_resolver: impl ChannelResolver + Send + Sync + 'static) -> Self {
        Self {
            channel_manager: Arc::new(ChannelManager::new(channel_resolver)),
            runtime: Arc::new(RuntimeEnv::default()),
            partitioner_registry: Arc::new(StreamPartitionerRegistry::default()),
            session_builder: Arc::new(NoopSessionBuilder),
        }
    }

    pub fn with_session_builder(
        &mut self,
        session_builder: impl SessionBuilder + Send + Sync + 'static,
    ) {
        self.session_builder = Arc::new(session_builder);
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
