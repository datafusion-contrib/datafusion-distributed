#![deny(clippy::all)]

mod common;
mod config_extension_ext;
mod distributed_ext;
mod execution_plans;
mod flight_service;
mod metrics;
mod stage;

mod distributed_planner;
mod networking;
mod observability;
mod protobuf;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use arrow_ipc::CompressionType;
pub use distributed_ext::DistributedExt;
pub use distributed_planner::{
    DistributedConfig, DistributedPhysicalOptimizerRule, NetworkBoundary, NetworkBoundaryExt,
    TaskCountAnnotation, TaskEstimation, TaskEstimator,
};
pub use execution_plans::{
    BroadcastExec, DistributedExec, NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec,
    PartitionIsolatorExec,
};
pub use flight_service::{
    DefaultSessionBuilder, MappedWorkerSessionBuilder, MappedWorkerSessionBuilderExt, TaskData,
    Worker, WorkerQueryContext, WorkerSessionBuilder,
};
pub use metrics::{DistributedMetricsFormat, rewrite_distributed_plan_with_metrics};
pub use networking::{
    BoxCloneSyncChannel, ChannelResolver, DefaultChannelResolver, WorkerResolver,
    create_flight_client, get_distributed_channel_resolver, get_distributed_worker_resolver,
};
pub use stage::{
    DistributedTaskContext, ExecutionTask, Stage, display_plan_ascii, display_plan_graphviz,
    explain_analyze,
};

pub use observability::{
    ConsoleControlService, ConsoleControlServiceClient, ConsoleControlServiceImpl,
    ConsoleControlServiceServer, GetTaskProgressRequest, GetTaskProgressResponse,
    ObservabilityService, ObservabilityServiceClient, ObservabilityServiceImpl,
    ObservabilityServiceServer, PingRequest, PingResponse, RegisterWorkersRequest,
    RegisterWorkersResponse, StageKey as ObservabilityStageKey, TaskProgress, TaskStatus,
};

pub use protobuf::StageKey;
