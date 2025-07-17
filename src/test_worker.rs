use arrow_flight::{flight_service_server::FlightService, FlightDescriptor, PollInfo};
use futures::stream::BoxStream;
use tonic::{async_trait, Request, Response, Status};
use futures::stream::{self};


pub struct TestWorker;

impl Default for TestWorker {
    fn default() -> Self {
        TestWorker
    }
}

#[async_trait]
impl FlightService for TestWorker {
    type HandshakeStream = BoxStream<'static, Result<arrow_flight::HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<arrow_flight::FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<arrow_flight::FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<arrow_flight::PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<arrow_flight::FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<arrow_flight::ActionType, Status>>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        // For tests we don’t need the long-running behaviour yet.
        // A simple “unimplemented” stub keeps the compiler happy.
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<arrow_flight::FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn do_get(
        &self,
        _req: Request<arrow_flight::Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get not implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    async fn do_action(
        &self,
        _req: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty())))
    }

    async fn list_actions(
        &self,
        _request: tonic::Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }
}