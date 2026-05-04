use crate::distributed_planner::network_boundary::network_boundary_scale_input;
use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::stage::LocalStage;
use crate::{NetworkBoundaryExt, Stage};
use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use uuid::Uuid;

/// Prepares every [NetworkBoundary] in the plan for distributed execution: elides ones whose
/// producer and consumer sides both run on a single task, scales the producer-stage head of
/// the survivors to feed all consumer tasks, and stamps each surviving stage with a unique
/// `(query_id, num)` identifier.
pub(crate) fn prepare_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    prepare(plan, 1, Uuid::new_v4(), &mut 1)
}

fn prepare(
    plan: Arc<dyn ExecutionPlan>,
    consumer_task_count: usize,
    query_id: Uuid,
    num: &mut usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    // A `ChildrenIsolatorUnionExec` runs each child in only a subset of the surrounding stage's
    // tasks. Boundaries living inside one of those children must scale by that child's own task
    // count, not the full stage's task count, otherwise hash partitioning is over-scaled and
    // data ends up routed to partitions no consumer reads.
    if let Some(ciu) = plan.as_any().downcast_ref::<ChildrenIsolatorUnionExec>() {
        let children_and_task_count = ciu.children().into_iter().zip(ciu.child_task_counts());
        let new_children = children_and_task_count
            .map(|(child, per_child_count)| {
                prepare(Arc::clone(child), per_child_count, query_id, num)
            })
            .collect::<Result<Vec<_>>>()?;
        return plan.with_new_children(new_children);
    }

    let Some(nb) = plan.as_network_boundary() else {
        let new_children = plan
            .children()
            .into_iter()
            .map(|c| prepare(Arc::clone(c), consumer_task_count, query_id, num))
            .collect::<Result<Vec<_>>>()?;
        return plan.with_new_children(new_children);
    };

    // If the input stage is already remote, it was already sent over the network, so nothing else
    // we can do here.
    let Stage::Local(local_stage) = nb.input_stage() else {
        return Ok(plan);
    };
    let producer_task_count = local_stage.tasks;
    let new_input = prepare(
        Arc::clone(&local_stage.plan),
        producer_task_count,
        query_id,
        num,
    )?;
    // 1) If there are both 1 producer and consumer tasks, optimize the network boundary out.
    if consumer_task_count == 1 && producer_task_count == 1 {
        return Ok(new_input);
    }
    let consumer_partitions = nb.properties().partitioning.partition_count();

    // 2) Scale up the head node of the input stage in order to account for the amount of partition
    //    and consumer count above it.
    let plan = network_boundary_scale_input(new_input, consumer_partitions, consumer_task_count)?;

    // 3) Make sure the input stage can be uniquely identified with a stage index and query id.
    //    If there were already some `query_id` and `num` that's fine.
    let nb = nb.with_input_stage(Stage::Local(LocalStage {
        query_id,
        num: *num,
        plan,
        tasks: local_stage.tasks,
    }));
    *num += 1;
    Ok(nb? as Arc<dyn ExecutionPlan>)
}
