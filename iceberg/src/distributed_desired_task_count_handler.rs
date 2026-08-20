use datafusion::common::stats::Precision;
use datafusion::common::{Result, config_datafusion_err};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion_distributed::{
    DesiredTaskCountEvent, DesiredTaskCountEventResponse, DistributedConfig,
};

use crate::IcebergDataSource;

/// Estimates scan parallelism from the selected Iceberg snapshot's total file size.
pub fn iceberg_desired_task_count(
    ev: DesiredTaskCountEvent,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    let iceberg_data_source = ev
        .plan
        .downcast_ref::<DataSourceExec>()?
        .data_source()
        .downcast_ref::<IcebergDataSource>()?;
    let distributed_config = match DistributedConfig::from_session_config(ev.session_config) {
        Ok(config) => config,
        Err(error) => return Some(Err(error)),
    };
    let statistics = match iceberg_data_source.partition_statistics(None) {
        Ok(statistics) => statistics,
        Err(error) => return Some(Err(error)),
    };
    let total_bytes = match statistics.total_byte_size {
        Precision::Exact(value) | Precision::Inexact(value) => value,
        Precision::Absent => return None,
    };
    let Some(bytes_per_task) = distributed_config
        .file_scan_config_bytes_per_partition
        .checked_mul(ev.session_config.target_partitions())
        .filter(|value| *value > 0)
    else {
        return Some(Err(config_datafusion_err!(
            "Iceberg bytes per task must be greater than zero and fit in usize"
        )));
    };
    let task_count = total_bytes.div_ceil(bytes_per_task).max(1);

    Some(Ok(DesiredTaskCountEventResponse::desired(task_count)))
}
