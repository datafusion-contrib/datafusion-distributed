#[cfg(feature = "grpc")]
pub mod grpc;

mod channel_resolver;
mod worker_channel;

pub use channel_resolver::{ChannelResolver, get_distributed_channel_resolver};
pub(crate) use channel_resolver::{ChannelResolverExtension, set_distributed_channel_resolver};

pub use worker_channel::{
    CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest, GetWorkerInfoResponse,
    LoadInfo, ProducerHeadSpec, SetPlanRequest, TaskKey, TaskMetrics, WorkUnitBatch,
    WorkUnitFeedDeclaration, WorkUnitMsg, WorkerChannel, WorkerToCoordinatorMsg,
};
