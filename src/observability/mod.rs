mod generated;
mod service;

pub use generated::observability::observability_service_client::ObservabilityServiceClient;
pub use generated::observability::observability_service_server::{
    ObservabilityService, ObservabilityServiceServer,
};

pub use generated::observability::{
    GetTaskProgressRequest, GetTaskProgressResponse, PingRequest, PingResponse, StageKey,
    TaskProgress, TaskStatus,
};
pub use service::ObservabilityServiceImpl;
