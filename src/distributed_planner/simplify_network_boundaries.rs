use crate::stage::LocalStage;
use crate::{NetworkBoundaryExt, Stage};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Walks the plan tree and elides any [NetworkBoundary] whose producer side and consumer side both
/// run on a single task. Such boundaries add a network hop without partitioning data across tasks,
/// so they can be replaced by their inner plan with no change in semantics.
///
/// The root of the plan is treated as having a `consumer_task_count` of `1` (i.e. the coordinator).
pub(crate) fn simplify_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    simplify(plan, 1)
}

fn simplify(
    plan: Arc<dyn ExecutionPlan>,
    consumer_task_count: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(nb) = plan.as_network_boundary() {
        let Stage::Local(local_stage) = nb.input_stage() else {
            return Ok(plan);
        };
        let producer_task_count = local_stage.tasks;
        let new_inner = simplify(Arc::clone(&local_stage.plan), producer_task_count)?;
        if consumer_task_count == 1 && producer_task_count == 1 {
            return Ok(new_inner);
        }
        nb.with_input_stage(Stage::Local(LocalStage {
            query_id: local_stage.query_id,
            num: local_stage.num,
            plan: new_inner,
            tasks: local_stage.tasks,
        }))
    } else {
        let new_children = plan
            .children()
            .into_iter()
            .map(|c| simplify(Arc::clone(c), consumer_task_count))
            .collect::<Result<Vec<_>>>()?;
        plan.with_new_children(new_children)
    }
}
