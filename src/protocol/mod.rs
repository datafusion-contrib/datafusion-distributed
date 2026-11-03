#[cfg(feature = "grpc")]
pub mod grpc;

mod channel_resolver;
mod in_process;
mod worker_channel;

/// Set by the gRPC client on every `CoordinatorChannel` and `ExecuteTask` call to the query id of
/// the [TaskKey] the call names, as a hyphenated UUID, overwriting a passthrough header of the same
/// name. A tower layer in front of a worker can authorize a call by its query from this header
/// without decoding the body.
///
/// The worker refuses a call that carries it more than once, or with a value other than the query
/// id of the [TaskKey] in the body. A call without it is served.
pub const TASK_KEY_QUERY_ID_HEADER: &str = "x-datafusion-distributed-task-key-query-id";

pub(crate) use channel_resolver::set_distributed_channel_resolver;

pub use channel_resolver::{ChannelResolver, get_distributed_channel_resolver};
pub use in_process::LocalWorkerContext;
pub use worker_channel::{
    ApplyDynamicFilter, CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest,
    GetWorkerInfoResponse, LoadInfo, ProducedDynamicFilter, SetPlanRequest,
    TaskCompletedDynamicFilters, TaskDynamicFilter, TaskKey, TaskMetrics, WorkUnitBatch,
    WorkUnitFeedDeclaration, WorkUnitMsg, WorkerChannel, WorkerToCoordinatorMsg,
};
