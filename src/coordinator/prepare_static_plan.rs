use crate::NetworkMaterializedScanExec;
use crate::common::TreeNodeExt;
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::dynamic_filtering::{
    is_remote_dynamic_filtering_enabled, orphan_dynamic_filter_consumers,
};
use crate::stage::RemoteStage;
use crate::{NetworkBoundaryExt, Stage};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{HashMap, Result, exec_err};
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use futures::future::{BoxFuture, try_join_all};
use std::sync::Arc;
use uuid::Uuid;

/// Prepares the distributed plan for execution, which implies:
/// 1. Perform some worker URL assignation, choosing either:
///    - The workers selected by the user's [crate::RouteTaskHandler].
///    - Randomly otherwise
/// 2. Sending the sliced subplans to the assigned URLs. For each URL assigned to a task, a
///    network call feeding the subplan is necessary.
/// 3. In each network boundary, set the input plan to `None`. That way, network boundaries
///    become nodes without children and traversing them will not go further down in.
/// 4. Spawn a background task per worker that waits for the worker to finish and collects
///    its metrics into [DistributedExec::task_metrics] via the coordinator channel.
pub(super) async fn prepare_static_plan(
    query_coordinator: &QueryCoordinator,
    base_plan: &Arc<dyn ExecutionPlan>,
) -> Result<PreparedPlan> {
    let head_stage = if base_plan.exists(|node| Ok(node.is::<NetworkMaterializedScanExec>()))? {
        prepare_node(
            query_coordinator,
            Arc::clone(base_plan),
            &mut HashMap::new(),
        )
        .await?
    } else {
        Arc::clone(base_plan)
            .transform_up_async(async |plan| {
                if !plan.is_network_boundary() {
                    return Ok(Transformed::no(plan));
                }
                Ok(Transformed::yes(
                    prepare_boundary(query_coordinator, plan).await?,
                ))
            })
            .await?
            .data
    };
    Ok(PreparedPlan {
        head_stage,
        plan_for_viz: Arc::clone(base_plan),
    })
}

/// Reuse prepared stages before descending: repeated references must neither assign new workers
/// nor resend the producer's plan (or any of its input stages).
fn prepare_node<'a>(
    query_coordinator: &'a QueryCoordinator,
    plan: Arc<dyn ExecutionPlan>,
    stages: &'a mut HashMap<(Uuid, usize), Stage>,
) -> BoxFuture<'a, Result<Arc<dyn ExecutionPlan>>> {
    Box::pin(async move {
        if let Some(boundary) = plan.as_network_boundary() {
            let input = boundary.input_stage();
            if let Some(stage) = stages.get(&(input.query_id(), input.num())) {
                return Ok(boundary.with_input_stage(stage.clone())? as Arc<dyn ExecutionPlan>);
            }
        }
        let mut children = Vec::new();
        for child in plan.children() {
            children.push(prepare_node(query_coordinator, Arc::clone(child), stages).await?);
        }
        let changed = plan
            .children()
            .iter()
            .zip(&children)
            .any(|(old, new)| !Arc::ptr_eq(old, new));
        let plan = if changed {
            plan.replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?
        } else {
            plan
        };
        let plan = prepare_boundary(query_coordinator, plan).await?;
        if let Some(boundary) = plan.as_network_boundary() {
            let stage = boundary.input_stage();
            stages.insert((stage.query_id(), stage.num()), stage.clone());
        }
        Ok(plan)
    })
}

async fn prepare_boundary(
    query_coordinator: &QueryCoordinator,
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(boundary) = plan.as_network_boundary() else {
        return Ok(plan);
    };
    let Stage::Local(stage) = boundary.input_stage() else {
        return exec_err!("Input stage from network boundary was not in Local state");
    };
    let dynamic_filter_anchors =
        if is_remote_dynamic_filtering_enabled(query_coordinator.session_config())? {
            orphan_dynamic_filter_consumers(&stage.plan)?
        } else {
            vec![]
        };

    let mut stage_coordinator = query_coordinator.stage_coordinator(stage);
    let mut futures = Vec::with_capacity(stage.tasks);
    for task_i in 0..stage.tasks {
        futures.push(stage_coordinator.init_bidirectional_stream(task_i));
    }
    let results = try_join_all(futures).await?;

    let mut workers = Vec::with_capacity(stage.tasks);
    for (task_i, (url, worker_tx, worker_rx)) in results.into_iter().enumerate() {
        workers.push(url);
        stage_coordinator.worker_to_coordinator_task(task_i, worker_rx);
        stage_coordinator.coordinator_to_worker_task(task_i, worker_tx)?;
    }

    stage_coordinator.seal_dynamic_filter_stage()?;
    let remote = Stage::Remote(RemoteStage {
        query_id: stage.query_id,
        num: stage.num,
        workers,
        runtime_stats: None,
        dynamic_filter_anchors,
    });
    Ok(boundary.with_input_stage(remote)? as Arc<dyn ExecutionPlan>)
}
