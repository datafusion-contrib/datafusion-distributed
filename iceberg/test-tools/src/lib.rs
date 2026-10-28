#![allow(clippy::disallowed_types)]

//! Builders for the Iceberg tables used in tests.
//!
//! A [`FixtureCatalog`] is an in-memory catalog over a temporary directory.
//! Tables are created in it with a [`TableBuilder`], and each
//! [`FixtureTable::append`] commits one snapshot whose data files are exactly
//! the batches it was given.
//!
//! ```no_run
//! # async fn example() -> datafusion::error::Result<()> {
//! use iceberg::TableIdent;
//! use iceberg::spec::Transform;
//! use datafusion_distributed_iceberg_test_tools::{FixtureCatalog, taxi_schema, taxi_trips};
//!
//! let fixtures = FixtureCatalog::try_new().await?;
//! let ident = TableIdent::from_strs(["public", "trips"]).expect("valid identifier");
//! let mut table = fixtures
//!     .table(ident, taxi_schema())
//!     .partition_by("pickup_date", Transform::Identity)
//!     .create()
//!     .await?;
//! table.append().file(taxi_trips(0)?).commit().await?;
//! # Ok(())
//! # }
//! ```

mod append;
mod catalog;
mod described;
mod error;
mod table;
mod taxi;

pub use append::Append;
pub use catalog::FixtureCatalog;
pub use described::{ColumnMetrics, DescribedFile};
pub use table::{FixtureTable, TableBuilder};
pub use taxi::*;
