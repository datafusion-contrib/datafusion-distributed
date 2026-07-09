#[cfg(feature = "grpc")]
pub mod grpc;

mod channel_resolver;
// The prost message types carry no tonic dependency, so a non-gRPC transport (an in-process
// worker, a shared-memory mesh) can speak the same wire shape without pulling in the whole gRPC
// stack.
pub(crate) mod generated;
mod in_process;
// The metrics codec sits off gRPC for the same reason as the message types: a transport that
// delivers metrics out-of-band decodes the same frames the gRPC client does.
pub(crate) mod metrics_proto;
mod worker_channel;

pub(crate) use channel_resolver::set_distributed_channel_resolver;
pub use channel_resolver::{ChannelResolver, get_distributed_channel_resolver};
pub use in_process::InProcessChannelResolver;
pub use metrics_proto::decode_task_metrics;

pub use worker_channel::{
    CoordinatorToWorkerMsg, ExecuteTaskRequest, GetWorkerInfoRequest, GetWorkerInfoResponse,
    LoadInfo, ProducerHeadSpec, SetPlanRequest, TaskKey, TaskMetrics, WorkUnitBatch,
    WorkUnitFeedDeclaration, WorkUnitMsg, WorkerChannel, WorkerToCoordinatorMsg,
};
