use crate::common::ttl_map::{TTLMap, TTLMapConfig};
use crate::flight_service::DistributedSessionBuilder;
use crate::flight_service::do_get::TaskData;
use crate::protobuf::StageKey;
use arrow_flight::flight_service_server::FlightService;
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
pub(super) struct ArrowFlightEndpointHooks {
    pub(super) on_plan:
        Vec<Arc<dyn Fn(Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> + Sync + Send>>,
}

pub struct ArrowFlightEndpoint {
    pub(super) runtime: Arc<RuntimeEnv>,
    pub(super) task_data_entries: Arc<TTLMap<StageKey, Arc<OnceCell<TaskData>>>>,
    pub(super) session_builder: Arc<dyn DistributedSessionBuilder + Send + Sync>,
    pub(super) hooks: ArrowFlightEndpointHooks,
    max_message_size: usize,
}

/// Default maximum message size for FlightData chunks in ArrowFlightEndpoint.
/// This is the size used for chunking FlightData within the endpoint.
const DEFAULT_MESSAGE_SIZE: usize = 2 * 1024 * 1024; // 2 MB

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
            max_message_size: DEFAULT_MESSAGE_SIZE,
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

    /// Set the maximum message size for FlightData chunks.
    /// Defaults to 2 MB.
    /// If you change this, ensure you configure the server's max_encoding_message_size and
    /// max_decoding_message_size to at least 2x this value to allow for overhead.
    /// If your service communication is purely internal and there is no risk of DOS attacks,
    /// you may want to set this to a considerably larger value to minimize the overhead of chunking
    /// larger datasets.
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
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
