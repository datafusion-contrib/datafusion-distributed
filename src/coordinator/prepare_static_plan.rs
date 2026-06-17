use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::stage::RemoteStage;
use crate::{NetworkBoundaryExt, Stage};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Prepares the distributed plan for execution, which implies:
/// 1. Perform some worker URL assignation, choosing either:
///    - The URLs set by the user with [crate::TaskEstimator::route_tasks].
///    - Randomly otherwise
/// 2. Sending the sliced subplans to the assigned URLs. For each URL assigned to a task, a
///    network call feeding the subplan is necessary.
/// 3. In each network boundary, set the input plan to `None`. That way, network boundaries
///    become nodes without children and traversing them will not go further down in.
/// 4. Spawn a background task per worker that waits for the worker to finish and collects
///    its metrics into [DistributedExec::task_metrics] via the coordinator channel.
pub(super) fn prepare_static_plan(
    query_coordinator: &QueryCoordinator,
    base_plan: &Arc<dyn ExecutionPlan>,
) -> Result<PreparedPlan> {
    let prepared = Arc::clone(base_plan).transform_up(|plan| {
        // The following logic is just applied on network boundaries.
        let Some(plan) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };

        let Stage::Local(stage) = plan.input_stage() else {
            return exec_err!("Input stage from network boundary was not in Local state");
        };

        let mut stage_coordinator = query_coordinator.stage_coordinator(stage);

        let routed_urls = stage_coordinator.routed_urls()?;

        let mut workers = Vec::with_capacity(stage.tasks);
        for (i, routed_url) in routed_urls.into_iter().enumerate() {
            workers.push(routed_url.clone());
            // Spawn a task that sends the subplan to the chosen URL.
            // There will be as many spawned tasks as workers.
            let (worker_tx, worker_rx) = stage_coordinator.send_plan_task(i, routed_url)?;
            stage_coordinator.worker_to_coordinator_task(i, worker_rx);
            stage_coordinator.coordinator_to_worker_task(i, worker_tx)?;
        }

        Ok(Transformed::yes(plan.with_input_stage(Stage::Remote(
            RemoteStage {
                query_id: stage.query_id,
                num: stage.num,
                workers,
            },
        ))?))
    })?;
    Ok(PreparedPlan {
        head_stage: prepared.data,
        // If the plan was statically planned, the base plan is the same one that will be used for
        // visualization.
        plan_for_viz: Arc::clone(base_plan),
    })
}
