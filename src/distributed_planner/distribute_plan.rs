use crate::common::require_one_child;
use crate::distributed_planner::batch_coalescing_below_network_boundaries::batch_coalescing_below_network_boundaries;
use crate::distributed_planner::insert_broadcast::insert_broadcast_execs;
use crate::distributed_planner::plan_annotator::{
    AnnotatedPlan, PlanOrNetworkBoundary, annotate_plan,
};
use crate::{
    DistributedConfig, DistributedExec, NetworkBoundaryExt, NetworkBroadcastExec,
    NetworkCoalesceExec, NetworkShuffleExec, TaskEstimator,
};
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::TreeNode;
use datafusion::config::ConfigOptions;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::ops::AddAssign;
use std::sync::Arc;
use uuid::Uuid;

/// Inspects the plan, places the appropriate network boundaries, and breaks it down into stages
/// that can be executed in a distributed manner.
///
/// It performs the following operations:
///
/// 1. It prepares the plan for distribution, adding some extra single-node nodes like
///    [BroadcastExec] or [CoalescePartitionsExec] that will signal the following steps to
///    introduce network boundaries in the appropriate places.
///
/// 2. Annotate the plan with [annotate_plan]: adds some annotations to each node about how
///    many distributed tasks should be used in the stage containing them, and whether they
///    need a network boundary below or not.
///    For more information about this step, read [annotate_plan] docs.
///
/// 3. Based on the [AnnotatedPlan] returned by [annotate_plan], place all the appropriate
///    network boundaries ([NetworkShuffleExec] and [NetworkCoalesceExec]) with the task count
///    assignation that the annotations required. After this, the plan is already a distributed
///    executable plan.
///
/// 4. Place the [CoalesceBatchesExec] in the appropriate places (just below network boundaries),
///    so that we send fewer and bigger record batches over the wire instead of a lot of small ones.
///
/// This function is idempotent: if a plan was already distributed, it will not be distributed
/// again.
pub async fn distribute_plan(
    original: Arc<dyn ExecutionPlan>,
    cfg: &ConfigOptions,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    if let Some(d_exec) = original.as_any().downcast_ref::<DistributedExec>() {
        let new_child = Box::pin(distribute_plan(Arc::clone(&d_exec.plan), cfg)).await?;

        // If there is no network boundary, then the plan was not distributed, therefore it's fine
        // to remove the DistributedExec head node and just keep the single-node plan.
        if !new_child.exists(|plan| Ok(plan.is_network_boundary()))? {
            return Ok(Arc::clone(&d_exec.plan));
        }

        return original.with_new_children(vec![new_child]);
    }
    // This function should be idempotent, as people need to be able to call this function at will,
    // and the DistributedExec node should not execute it's content again if it was already called.
    if original.exists(|plan| Ok(plan.is_network_boundary()))? {
        return Ok(original);
    }

    let mut plan = Arc::clone(&original);

    // Add a CoalescePartitionsExec on top of the plan if necessary. The plan annotator will see
    // this and will place a NetworkCoalesceExec below it.
    if plan.output_partitioning().partition_count() > 1 {
        plan = Arc::new(CoalescePartitionsExec::new(plan));
    }

    // Insert BroadcastExec nodes in collect left joins so that the plan annotator can inject
    // broadcast network boundaries above.
    plan = insert_broadcast_execs(plan, cfg)?;

    // Annotate the plan with network boundary and task count information.
    let annotated = annotate_plan(plan, cfg).await?;

    // Based on the annotations, place the actual network boundaries with the appropriate dimensions.
    let mut stage_id = 1;
    let plan = _distribute_plan(annotated, cfg, Uuid::new_v4(), &mut stage_id)?;
    if stage_id == 1 {
        return Ok(original);
    }

    // Place some batch coalescing nodes before network boundaries in order to send big batches
    // over the wire.
    // TODO: This should be removed after DataFusion 53 upgrade, as CoalesceBatchesExec is deprecated.
    let plan = batch_coalescing_below_network_boundaries(plan, cfg)?;

    Ok(plan)
}

/// Takes an [AnnotatedPlan] and returns a modified [ExecutionPlan] with all the network boundaries
/// appropriately placed. This step performs the following modifications to the original
/// [ExecutionPlan]:
/// - The leaf nodes are scaled up in parallelism based on the number of distributed tasks in
///   which they are going to run. This is configurable by the user via the [TaskEstimator] trait.
/// - The appropriate network boundaries are placed in the plan depending on how it was annotated,
///   so new nodes like [NetworkBroadcastExec], [NetworkCoalesceExec] and [NetworkShuffleExec] will be present.
fn _distribute_plan(
    annotated_plan: AnnotatedPlan,
    cfg: &ConfigOptions,
    query_id: Uuid,
    stage_id: &mut usize,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    let d_cfg = DistributedConfig::from_config_options(cfg)?;
    let children = annotated_plan.children;
    let task_count = annotated_plan.task_count.as_usize();
    let max_child_task_count = children.iter().map(|v| v.task_count.as_usize()).max();
    let new_children = children
        .into_iter()
        .map(|child| _distribute_plan(child, cfg, query_id, stage_id))
        .collect::<Result<Vec<_>, _>>()?;
    match annotated_plan.plan_or_nb {
        // This is a leaf node. It needs to be scaled up in order to account for it running in
        // multiple tasks.
        PlanOrNetworkBoundary::Plan(plan) if plan.children().is_empty() => {
            let scaled_up = d_cfg.__private_task_estimator.scale_up_leaf_node(
                &plan,
                annotated_plan.task_count.as_usize(),
                cfg,
            );
            Ok(scaled_up.unwrap_or(plan))
        }
        // This is a normal intermediate plan, just pass it through with the mapped children.
        PlanOrNetworkBoundary::Plan(plan) => plan.with_new_children(new_children),
        // This is a shuffle, so inject a NetworkShuffleExec here in the plan.
        PlanOrNetworkBoundary::Shuffle => {
            // It would need a network boundary, but on both sides of the boundary there is just 1 task,
            // so we are fine with not introducing any network boundary.
            if task_count == 1 && max_child_task_count == Some(1) {
                return require_one_child(new_children);
            }
            let node = Arc::new(NetworkShuffleExec::try_new(
                require_one_child(new_children)?,
                query_id,
                *stage_id,
                task_count,
                max_child_task_count.unwrap_or(1),
            )?);
            stage_id.add_assign(1);
            Ok(node)
        }
        // DataFusion is trying to coalesce multiple partitions into one, so we should do the
        // same with tasks.
        PlanOrNetworkBoundary::Coalesce => {
            // It would need a network boundary, but on both sides of the boundary there is just 1 task,
            // so we are fine with not introducing any network boundary.
            if task_count == 1 && max_child_task_count == Some(1) {
                return require_one_child(new_children);
            }
            let node = Arc::new(NetworkCoalesceExec::try_new(
                require_one_child(new_children)?,
                query_id,
                *stage_id,
                task_count,
                max_child_task_count.unwrap_or(1),
            )?);
            stage_id.add_assign(1);
            Ok(node)
        }
        // This is a CollectLeft HashJoinExec with the build side marked as being broadcast. we
        // need to insert a NetworkBroadcastExec and scale up the BroadcastExec consumer_tasks.
        PlanOrNetworkBoundary::Broadcast => {
            // It would need a network boundary, but on both sides of the boundary there is just 1 task,
            // so we are fine with not introducing any network boundary.
            if task_count == 1 && max_child_task_count == Some(1) {
                return require_one_child(new_children);
            }
            let node = Arc::new(NetworkBroadcastExec::try_new(
                require_one_child(new_children)?,
                query_id,
                *stage_id,
                task_count,
                max_child_task_count.unwrap_or(1),
            )?);
            stage_id.add_assign(1);
            Ok(node)
        }
    }
}
