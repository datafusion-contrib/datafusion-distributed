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

use parquet_layout::TableColumns;

/// Returns the columns that benchmark `suite` lists for `table`: its sort keys, its dictionary
/// columns and its partition columns. Any other suite lists none.
pub fn table_columns(suite: &str, table: &str) -> TableColumns {
    match suite {
        "tpch" => tpch::table_columns(table),
        "tpcds" => tpcds::table_columns(table),
        _ => TableColumns::default(),
    }
}
