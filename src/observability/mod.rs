mod generated;
mod service;

pub use generated::observability::observability_service_client::ObservabilityServiceClient;
pub use generated::observability::observability_service_server::{
    ObservabilityService, ObservabilityServiceServer,
};

pub use generated::observability::{
    GetTaskMetricsRequest, GetTaskMetricsResponse, PingRequest, PingResponse, StageKey,
    TaskMetricsSummary,
};
pub use service::ObservabilityServiceImpl;
