#![deny(clippy::all)]

mod channel_resolver_ext;
mod common;
mod config_extension_ext;
mod distributed_ext;
mod execution_plans;
mod flight_service;
mod metrics;
mod stage;

mod distributed_planner;
mod protobuf;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use channel_resolver_ext::{BoxCloneSyncChannel, ChannelResolver};
pub use distributed_ext::DistributedExt;
pub use distributed_planner::{
    DistributedConfig, DistributedPhysicalOptimizerRule, InputStageInfo, NetworkBoundary,
    NetworkBoundaryExt, apply_network_boundaries, distribute_plan,
};
pub use execution_plans::{
    DistributedExec, NetworkCoalesceExec, NetworkShuffleExec, PartitionIsolatorExec,
};
pub use flight_service::{
    ArrowFlightEndpoint, DefaultSessionBuilder, DistributedSessionBuilder,
    DistributedSessionBuilderContext, MappedDistributedSessionBuilder,
    MappedDistributedSessionBuilderExt,
};
pub use metrics::rewrite_distributed_plan_with_metrics;
pub use stage::{
    DistributedTaskContext, ExecutionTask, Stage, display_plan_ascii, display_plan_graphviz,
    explain_analyze,
};
