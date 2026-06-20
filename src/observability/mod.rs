mod generated;
#[cfg(feature = "flight")]
mod service;

#[cfg(feature = "flight")]
pub use generated::observability::observability_service_client::ObservabilityServiceClient;
#[cfg(feature = "flight")]
pub use generated::observability::observability_service_server::{
    ObservabilityService, ObservabilityServiceServer,
};

pub use generated::observability::{
    GetClusterWorkersRequest, GetClusterWorkersResponse, GetTaskProgressRequest,
    GetTaskProgressResponse, PingRequest, PingResponse, TaskProgress, TaskStatus, WorkerMetrics,
};
#[cfg(feature = "flight")]
pub use service::ObservabilityServiceImpl;
