pub mod clickbench;
pub(crate) mod common;
pub mod output;
pub mod parquet_layout;
mod progress;
pub mod tpcds;
pub mod tpch;

pub use common::register_tables;

use datafusion::common::not_impl_err;
use datafusion::error::DataFusionError;
use parquet_layout::TableColumns;
use std::path::Path;

/// The query suite of a dataset path such as `tpch_parquet/scale_factor=1/...`: the benchmark
/// name in its last component that is not a `key=value` parameter.
pub fn dataset_suite(dataset: &str) -> &str {
    let suite = Path::new(dataset)
        .components()
        .rev()
        .filter_map(|component| component.as_os_str().to_str())
        .find(|name| !name.contains('='))
        .unwrap_or("");
    suite
        .split_once('_')
        .map_or(suite, |(suite, _format)| suite)
}

/// The columns of `table` that layouts treat specially, as the benchmark `suite` declares them.
pub fn table_columns(suite: &str, table: &str) -> Result<TableColumns, DataFusionError> {
    match suite {
        "tpch" => Ok(tpch::table_columns(table)),
        "tpcds" => Ok(tpcds::table_columns(table)),
        "clickbench" => Ok(TableColumns::default()),
        _ => not_impl_err!("Unknown benchmark dataset {suite}"),
    }
}
