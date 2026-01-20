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
pub mod protobuf;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use distributed_ext::DistributedExt;
pub use distributed_planner::{
    DistributedConfig, DistributedPhysicalOptimizerRule, NetworkBoundary, NetworkBoundaryExt,
    TaskCountAnnotation, TaskEstimation, TaskEstimator,
};
pub use execution_plans::{
    DistributedExec, NetworkCoalesceExec, NetworkShuffleExec, PartitionIsolatorExec,
};
pub use flight_service::{
    DefaultSessionBuilder, MappedWorkerSessionBuilder, MappedWorkerSessionBuilderExt, SharedWorker,
    Worker, WorkerQueryContext, WorkerSessionBuilder,
};
pub use metrics::rewrite_distributed_plan_with_metrics;
pub use networking::{
    BoxCloneSyncChannel, ChannelResolver, DefaultChannelResolver, WorkerResolver,
    create_flight_client,
};
pub use stage::{
    DistributedTaskContext, ExecutionTask, Stage, display_plan_ascii, display_plan_graphviz,
    explain_analyze,
};

pub use crate::protobuf::observability::{
    PingRequest, PingResponse, observability_service_client::ObservabilityServiceClient,
    observability_service_server::ObservabilityServiceServer,
};
