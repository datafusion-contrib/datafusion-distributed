#![deny(clippy::all)]

mod channel_resolver_ext;
mod common;
mod config_extension_ext;
mod distributed_ext;
mod distributed_physical_optimizer_rule;
mod execution_plans;
mod flight_service;
mod metrics;
mod stage;

mod protobuf;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use channel_resolver_ext::{BoxCloneSyncChannel, ChannelResolver};
pub use distributed_ext::DistributedExt;
pub use distributed_physical_optimizer_rule::{
    DistributedPhysicalOptimizerRule, NetworkBoundaryExt,
};
pub use execution_plans::{
    DistributedExec, NetworkCoalesceExec, NetworkShuffleExec, PartitionIsolatorExec,
};
pub use flight_service::{
    ArrowFlightEndpoint, DefaultSessionBuilder, DistributedSessionBuilder,
    DistributedSessionBuilderContext, MappedDistributedSessionBuilder,
    MappedDistributedSessionBuilderExt,
};
pub use stage::{
    DistributedTaskContext, ExecutionTask, Stage, display_plan_ascii, display_plan_graphviz,
};
