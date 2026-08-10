use datafusion::common::Result;
use datafusion::datasource::source::DataSourceExec;
use datafusion_distributed::{DesiredTaskCountEvent, DesiredTaskCountEventResponse};

use crate::IcebergDataSource;

// TODO: read the inner iceberg::table::Table and based on the contents attempt
//  to estimate a good desired task count.
pub fn iceberg_desired_task_count(
    ev: DesiredTaskCountEvent,
) -> Option<Result<DesiredTaskCountEventResponse>> {
    let _iceberg_data_source = ev
        .plan
        .downcast_ref::<DataSourceExec>()?
        .data_source()
        .downcast_ref::<IcebergDataSource>()?;

    // TODO: not implemented.

    None
}
