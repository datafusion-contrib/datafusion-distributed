#![allow(clippy::disallowed_types)]

//! Read-only Apache Iceberg integration for DataFusion Distributed.
//!
//! This crate ports the read path from Apache Iceberg Rust's DataFusion
//! integration. It deliberately contains no distributed execution adaptation
//! and no Iceberg write or commit implementation.

mod common;
mod config;
mod data_source;
mod distributed_desired_task_count_handler;
mod iceberg_ext;
mod table_provider;
mod work_unit_feed;
mod work_unit_wire;

mod codec;
#[doc(hidden)]
pub mod test_utils;

pub use codec::IcebergCodec;
pub use config::IcebergConfig;
pub use data_source::IcebergDataSource;
pub use distributed_desired_task_count_handler::iceberg_desired_task_count;
pub use iceberg_ext::IcebergExt;
pub use iceberg_ext::IcebergIntegrationOptions;
pub use table_provider::IcebergCatalogTableProvider;
pub use table_provider::IcebergStaticTableProvider;
pub use table_provider::IcebergTableProviderFactory;
pub use work_unit_feed::IcebergWorkUnitFeed;

// re-export of iceberg-rust.
pub use iceberg;
