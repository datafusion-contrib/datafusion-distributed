#[cfg(feature = "grpc")]
pub mod grpc;

mod channel_resolver;
// The prost message types carry no tonic dependency, so a non-gRPC transport (an in-process
// worker, a shared-memory mesh) can speak the same wire shape without pulling in the whole gRPC
// stack.
pub(crate) mod generated;
mod worker_channel;

pub(crate) use channel_resolver::set_distributed_channel_resolver;
pub use channel_resolver::{ChannelResolver, get_distributed_channel_resolver};

pub use worker_channel::{
    CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest, GetWorkerInfoResponse,
    LoadInfo, ProducerHeadSpec, SetPlanRequest, TaskKey, TaskMetrics, WorkUnitBatch,
    WorkUnitFeedDeclaration, WorkUnitMsg, WorkerChannel, WorkerToCoordinatorMsg,
};
