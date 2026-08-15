pub mod clickbench;
mod common;
pub mod tpcds;
mod tpcds_gen;
mod tpcds_schema;
pub mod tpch;

pub use common::register_tables;
