mod generated;
mod service;

pub use generated::observability::console_control_service_client::ConsoleControlServiceClient;
pub use generated::observability::console_control_service_server::{
    ConsoleControlService, ConsoleControlServiceServer,
};
pub use generated::observability::observability_service_client::ObservabilityServiceClient;
pub use generated::observability::observability_service_server::{
    ObservabilityService, ObservabilityServiceServer,
};

pub use generated::observability::{
    GetTaskProgressRequest, GetTaskProgressResponse, PingRequest, PingResponse,
    RegisterWorkersRequest, RegisterWorkersResponse, StageKey, TaskProgress, TaskStatus,
};
pub use service::{ConsoleControlServiceImpl, ObservabilityServiceImpl};
