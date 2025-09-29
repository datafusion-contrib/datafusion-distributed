#![deny(clippy::all)]

mod channel_resolver_ext;
mod common;
mod config_extension_ext;
mod distributed_ext;
mod distributed_physical_optimizer_rule;
mod execution_plans;
mod flight_service;
mod metrics;

mod protobuf;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use channel_resolver_ext::{BoxCloneSyncChannel, ChannelResolver};
pub use distributed_ext::DistributedExt;
pub use distributed_physical_optimizer_rule::DistributedPhysicalOptimizerRule;
pub use execution_plans::display_plan_graphviz;
pub use execution_plans::{
    DistributedTaskContext, ExecutionTask, NetworkCoalesceExec, NetworkShuffleExec,
    PartitionIsolatorExec, StageExec,
};
pub use flight_service::{
    ArrowFlightEndpoint, DefaultSessionBuilder, DistributedSessionBuilder,
    DistributedSessionBuilderContext, MappedDistributedSessionBuilder,
    MappedDistributedSessionBuilderExt,
};
