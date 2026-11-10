pub mod clickbench;
pub(crate) mod common;
pub mod output;
pub mod parquet_layout;
pub mod prepare_clickbench;
pub mod prepare_tpcds;
pub mod prepare_tpch;
mod progress;
pub mod tpcds;
pub mod tpch;

pub use common::register_tables;
