#![allow(clippy::disallowed_types)]

//! Read-only Apache Iceberg integration for DataFusion Distributed.
//!
//! This crate ports the read path from Apache Iceberg Rust's DataFusion
//! integration, with distributed scan assignment and work delivery. It contains
//! no production Iceberg write or commit implementation.

mod common;
mod config;
mod distributed_desired_task_count_handler;
mod distributed_scale_up_handler;
mod eager;
mod iceberg_ext;
mod lazy;
mod proto;
mod table_provider;

#[cfg(any(feature = "integration", test))]
#[doc(hidden)]
pub mod test_utils;

pub use config::IcebergConfig;
pub use distributed_desired_task_count_handler::iceberg_desired_task_count;
pub use eager::EagerIcebergCodec;
pub use eager::EagerIcebergDataSource;
pub use iceberg_ext::IcebergExt;
pub use iceberg_ext::IcebergIntegrationOptions;
pub use lazy::IcebergWorkUnitFeed;
pub use lazy::LazyIcebergCodec;
pub use lazy::LazyIcebergDataSource;
pub use table_provider::IcebergCatalogTableProvider;
pub use table_provider::IcebergStaticTableProvider;
pub use table_provider::IcebergTableProviderFactory;

// re-export of iceberg-rust.
pub use iceberg;
