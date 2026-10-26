#![allow(clippy::disallowed_types)]

//! Read-only Apache Iceberg integration for DataFusion Distributed.
//!
//! This crate ports the read path from Apache Iceberg Rust's DataFusion
//! integration, with distributed scan assignment and work delivery. It contains
//! no production Iceberg write or commit implementation.

mod assigned_codec;
mod assigned_data_source;
mod column_statistics;
mod common;
mod config;
mod data_source;
mod distributed_desired_task_count_handler;
mod distributed_scale_up_handler;
mod iceberg_ext;
mod planned_files;
mod proto;
mod scan;
mod table_provider;
mod work_unit_feed;

mod codec;
#[cfg(any(feature = "integration", test))]
#[doc(hidden)]
pub mod test_utils;

pub use assigned_codec::AssignedIcebergCodec;
pub use assigned_data_source::AssignedIcebergDataSource;
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
