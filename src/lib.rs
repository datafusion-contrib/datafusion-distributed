#![deny(clippy::all)]
extern crate core;

mod common;
mod config_extension_ext;
mod distributed_ext;
mod execution_plans;
mod flight_service;
mod metrics;
mod passthrough_headers;
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
    ComputeCostClass, DistributedConfig, DistributedPhysicalOptimizerRule,
    DistributedPlannerExtension, NetworkBoundary, NetworkBoundaryExt,
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
    ObservabilityService, ObservabilityServiceClient, ObservabilityServiceImpl,
    ObservabilityServiceServer, PingRequest, PingResponse,
};
