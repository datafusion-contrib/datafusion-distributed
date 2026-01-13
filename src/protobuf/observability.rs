use tonic::{Request, Response, Status};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingRequest {}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PingResponse {
    #[prost(uint32, tag = "1")]
    pub value: u32,
}

#[tonic::async_trait]
pub trait ObservabilityService: Send + Sync + 'static {
    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status>;
}
