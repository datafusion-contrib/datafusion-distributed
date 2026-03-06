mod generated;
mod service;

pub use generated::observability::observability_service_client::ObservabilityServiceClient;
pub use generated::observability::observability_service_server::{
    ObservabilityService, ObservabilityServiceServer,
};

pub use generated::observability::{
    GetClusterWorkersRequest, GetClusterWorkersResponse, GetTaskProgressRequest,
    GetTaskProgressResponse, PingRequest, PingResponse, StageKey, TaskProgress, TaskStatus,
    WorkerMetrics,
};
pub use service::ObservabilityServiceImpl;
