use std::sync::Arc;

use datafusion::common::Result;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DistributedLeafExec, ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};

use crate::AssignedIcebergDataSource;

/// Like DFD's FileScanConfig scale-up handler, build one scan variant per task
/// before routing. File assignment belongs to the source, not to the handler.
pub(crate) fn iceberg_scale_up(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let exec = ev.plan.downcast_ref::<DataSourceExec>()?;
    let source = exec
        .data_source()
        .downcast_ref::<AssignedIcebergDataSource>()?;
    if ev.task_count <= 1 {
        return None;
    }
    Some((|| {
        let variants = source
            .specialize(ev.task_count)?
            .into_iter()
            .map(|source| DataSourceExec::from_data_source(source) as Arc<dyn ExecutionPlan>);
        let plan = DistributedLeafExec::try_new(Arc::clone(ev.plan), variants)?;
        Ok(ScaleUpLeafNodeEventResponse::new(Arc::new(plan)))
    })())
}
