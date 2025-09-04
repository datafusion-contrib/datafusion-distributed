#![deny(clippy::all)]

mod channel_manager_ext;
mod common;
mod config_extension_ext;
mod distributed_ext;
mod errors;
mod execution_plans;
mod flight_service;
mod physical_optimizer;
mod stage;
mod task;
mod user_codec_ext;

#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use channel_manager_ext::{BoxCloneSyncChannel, ChannelResolver};
pub use distributed_ext::DistributedExt;
pub use execution_plans::{ArrowFlightReadExec, PartitionIsolatorExec};
pub use flight_service::{
    ArrowFlightEndpoint, DefaultSessionBuilder, DistributedSessionBuilder,
    DistributedSessionBuilderContext, MappedDistributedSessionBuilder,
    MappedDistributedSessionBuilderExt,
};
pub use physical_optimizer::DistributedPhysicalOptimizerRule;
pub use stage::{display_stage_graphviz, ExecutionStage};
