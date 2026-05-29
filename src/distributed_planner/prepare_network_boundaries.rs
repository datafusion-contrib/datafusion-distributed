use crate::common::TreeNodeExt;
use crate::distributed_planner::network_boundary::network_boundary_scale_input;
use crate::stage::LocalStage;
use crate::{NetworkBoundaryExt, Stage};
use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uuid::Uuid;

/// Prepares every [NetworkBoundary] in the plan for distributed execution:
/// - Elides ones whose producer and consumer sides both run on a single task
/// - Scales the producer-stage head of the survivors to feed all consumer tasks
/// - Stamps each surviving stage with a unique `(query_id, num)` identifier.
pub(crate) fn prepare_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut stage_id = 1;
    let query_id = Uuid::new_v4();

    let transformed = plan.transform_up_with_task_count(1, |plan, task_count| {
        let Some(nb) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };
        // If the input stage is already remote, it was already sent over the network, so nothing else
        // we can do here.
        let Stage::Local(input_stage) = nb.input_stage() else {
            return Ok(Transformed::no(plan));
        };

        // 1) If there are both 1 producer and consumer tasks, optimize the network boundary out.
        if task_count == 1 && input_stage.tasks == 1 {
            return Ok(Transformed::yes(Arc::clone(&input_stage.plan)));
        }

        // 2) Scale up the head node of the input stage in order to account for the amount of partition
        //    and consumer count above it.
        let plan = network_boundary_scale_input(
            Arc::clone(&input_stage.plan),
            nb.properties().partitioning.partition_count(),
            task_count,
        )?;

        // 3) Make sure the input stage can be uniquely identified with a stage index and query id.
        //    If there were already some `query_id` and `num` that's fine.
        let nb = nb.with_input_stage(Stage::Local(LocalStage {
            query_id,
            num: stage_id,
            plan,
            tasks: input_stage.tasks,
        }))?;
        stage_id += 1;
        Ok(Transformed::yes(nb))
    })?;

    Ok(transformed.data)
}
