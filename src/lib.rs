#![deny(clippy::all)]

mod channel_resolver_ext;
mod common;
mod config_extension_ext;
mod distributed_ext;
mod errors;
mod execution_plans;
mod flight_service;
mod physical_optimizer;

mod protobuf;
#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use channel_resolver_ext::{BoxCloneSyncChannel, ChannelResolver};
pub use distributed_ext::DistributedExt;
pub use execution_plans::{
    display_stage_graphviz, ArrowFlightReadExec, ExecutionTask, PartitionIsolatorExec, StageExec,
};
pub use flight_service::{
    ArrowFlightEndpoint, DefaultSessionBuilder, DistributedSessionBuilder,
    DistributedSessionBuilderContext, MappedDistributedSessionBuilder,
    MappedDistributedSessionBuilderExt,
};
pub use physical_optimizer::DistributedPhysicalOptimizerRule;
