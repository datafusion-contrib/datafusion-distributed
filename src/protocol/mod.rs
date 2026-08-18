#[cfg(feature = "grpc")]
pub mod grpc;

mod channel_resolver;
mod in_process;
mod worker_channel;

pub(crate) use channel_resolver::set_distributed_channel_resolver;

pub use channel_resolver::{ChannelResolver, get_distributed_channel_resolver};
pub use in_process::LocalWorkerContext;
pub use worker_channel::{
    CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest, GetWorkerInfoResponse,
    LoadInfo, SetPlanRequest, TaskKey, TaskMetrics, WorkUnitBatch, WorkUnitFeedDeclaration,
    WorkUnitMsg, WorkerChannel, WorkerToCoordinatorMsg,
};
