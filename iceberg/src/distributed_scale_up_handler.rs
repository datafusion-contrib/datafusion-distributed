use std::sync::Arc;

use datafusion::common::Result;
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DistributedLeafExec, ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};

use crate::EagerIcebergDataSource;
use crate::common::assign_files;

/// Like DFD's FileScanConfig scale-up handler, build one scan variant per task
/// before routing. This handler chooses task assignments; each source retains
/// the resulting local groups for routing, transport, and execution.
pub(crate) fn iceberg_scale_up(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let exec = ev.plan.downcast_ref::<DataSourceExec>()?;
    let source = exec
        .data_source()
        .downcast_ref::<EagerIcebergDataSource>()?;
    if ev.task_count <= 1 {
        return None;
    }
    Some((|| {
        let partitions_per_task = source.output_partitioning().partition_count();
        let assignments = assign_files(source.planned_files(), ev.task_count, partitions_per_task)?;
        let sources = assignments
            .into_iter()
            .map(|groups| source.with_file_groups(groups))
            .collect::<Result<Vec<_>>>()?;
        let variants = sources
            .into_iter()
            .map(|source| DataSourceExec::from_data_source(source) as Arc<dyn ExecutionPlan>);
        let plan = DistributedLeafExec::try_new(Arc::clone(ev.plan), variants)?;
        Ok(ScaleUpLeafNodeEventResponse::new(Arc::new(plan)))
    })())
}
