mod channel_resolver;
mod errors;
pub mod generated;
mod metrics_proto;
mod observability;
mod on_drop_stream;
mod spawn_select_all;
#[cfg(any(test, feature = "integration"))]
pub mod test_utils;
mod worker_client;
mod worker_service;

// TODO: this should not be exposed.
pub(crate) use channel_resolver::DEFAULT_CHANNEL_RESOLVER_PER_RUNTIME;

pub use channel_resolver::{BoxCloneSyncChannel, DefaultChannelResolver};
pub use observability::{
    GetClusterWorkersRequest, GetClusterWorkersResponse, GetTaskProgressRequest,
    GetTaskProgressResponse, ObservabilityService, ObservabilityServiceClient,
    ObservabilityServiceImpl, ObservabilityServiceServer, PingRequest, PingResponse, TaskProgress,
    TaskStatus, WorkerMetrics,
};
pub use worker_client::create_worker_client;
