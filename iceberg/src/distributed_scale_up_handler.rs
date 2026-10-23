use std::sync::Arc;

use datafusion::common::Result;
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed::{
    DistributedLeafExec, ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};

use crate::{AssignedFiles, IcebergConfig, IcebergDataSource, ScanInput};

/// Materialize eager assignments after the planner has chosen the final task count.
pub(crate) fn iceberg_scale_up(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let exec = ev.plan.downcast_ref::<DataSourceExec>()?;
    let source = exec.data_source().downcast_ref::<IcebergDataSource>()?;
    let files = source.assigned_files()?;
    if ev.task_count <= 1 {
        return None;
    }
    Some((|| {
        let assignments = AssignedFiles::assign(
            Arc::clone(&files.planned),
            ev.task_count,
            files.partition_count(),
            IcebergConfig::from_session_config(ev.session_config).greedy_file_assignment,
        )?;
        let variants = assignments
            .into_iter()
            .map(|files| {
                let mut source = source.clone();
                source.input = ScanInput::AssignedFiles(files);
                // Whole-scan column aggregates cannot describe a task's subset.
                source.column_stats = None;
                source.metrics = Default::default();
                DataSourceExec::from_data_source(source) as Arc<dyn ExecutionPlan>
            })
            .collect::<Vec<Arc<dyn ExecutionPlan>>>();
        let plan = DistributedLeafExec::try_new(Arc::clone(ev.plan), variants)?;
        Ok(ScaleUpLeafNodeEventResponse::new(Arc::new(plan)))
    })())
}
