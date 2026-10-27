use std::sync::Arc;

use datafusion::common::{Result, exec_err};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DistributedLeafExec, ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};

use crate::EagerIcebergDataSource;
use crate::common::file_partition;

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
        if ev
            .task_count
            .checked_mul(partitions_per_task)
            .is_none_or(|n| n == 0)
        {
            return exec_err!("Invalid Iceberg assignment partition count");
        }
        let mut assignments = vec![vec![Vec::new(); partitions_per_task]; ev.task_count];
        for position in 0..source.planned_files().len() {
            let partition = file_partition(position, ev.task_count, partitions_per_task);
            assignments[partition / partitions_per_task][partition % partitions_per_task]
                .push(position);
        }
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
