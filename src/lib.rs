#![deny(clippy::all)]

mod channel_manager;
mod common;
mod composed_extension_codec;
mod config_extension_ext;
mod errors;
mod flight_service;
mod physical_optimizer;
mod plan;
mod stage;
mod task;
mod user_provided_codec;

#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use channel_manager::{BoxCloneSyncChannel, ChannelManager, ChannelResolver};
pub use config_extension_ext::ConfigExtensionExt;
pub use flight_service::{ArrowFlightEndpoint, NoopSessionBuilder, SessionBuilder};
pub use physical_optimizer::DistributedPhysicalOptimizerRule;
pub use plan::ArrowFlightReadExec;
pub use stage::{display_stage_graphviz, ExecutionStage};
pub use user_provided_codec::{add_user_codec, with_user_codec};
