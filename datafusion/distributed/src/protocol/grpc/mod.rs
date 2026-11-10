mod channel_resolver;
mod errors;
mod generated;
mod observability;
mod spawn_select_all;
mod worker_client;
mod worker_service;

/// Set by the gRPC client on every `CoordinatorChannel` and `ExecuteTask` call to the query id of
/// the [TaskKey](crate::TaskKey) the call names, as a hyphenated UUID, overwriting a passthrough
/// header of the same name. A tower layer in front of a worker can authorize a call by its query
/// from this header without decoding the body.
///
/// The worker refuses a call that carries it more than once, or with a value other than the query
/// id of the [TaskKey](crate::TaskKey) in the body. A call without it is served.
pub const TASK_KEY_QUERY_ID_HEADER: &str = "x-datafusion-distributed-task-key-query-id";

// TODO: this should not be exposed.
pub(crate) use channel_resolver::DEFAULT_CHANNEL_RESOLVER_PER_RUNTIME;

pub use channel_resolver::{BoxCloneSyncChannel, DefaultChannelResolver};
pub use generated::worker::worker_service_client::WorkerServiceClient;
pub use observability::{
    GetClusterWorkersRequest, GetClusterWorkersResponse, GetTaskProgressRequest,
    GetTaskProgressResponse, ObservabilityService, ObservabilityServiceClient,
    ObservabilityServiceImpl, ObservabilityServiceServer, PingRequest, PingResponse, TaskProgress,
    TaskStatus, WorkerMetrics,
};
pub use worker_client::create_worker_client;
