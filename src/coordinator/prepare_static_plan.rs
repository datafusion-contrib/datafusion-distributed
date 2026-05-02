use crate::coordinator::MetricsStore;
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::task_spawner::{
    CoordinatorToWorkerMetrics, CoordinatorToWorkerTaskSpawner,
};
use crate::stage::RemoteStage;
use crate::{DistributedCodec, NetworkBoundaryExt, Stage, get_distributed_worker_resolver};
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use rand::Rng;
use std::sync::Arc;

/// Prepares the distributed plan for execution, which implies:
/// 1. Perform some worker assignation, choosing randomly from the given URLs and assigning one
///    URL per task.
/// 2. Sending the sliced subplans to the assigned URLs. For each URL assigned to a task, a
///    network call feeding the subplan is necessary.
/// 3. In each network boundary, set the input plan to `None`. That way, network boundaries
///    become nodes without children and traversing them will not go further down in.
/// 4. Spawn a background task per worker that waits for the worker to finish and collects
///    its metrics into [DistributedExec::task_metrics] via the coordinator channel.
pub(super) fn prepare_static_plan(
    base_plan: &Arc<dyn ExecutionPlan>,
    metrics: &ExecutionPlanMetricsSet,
    task_metrics: &Arc<MetricsStore>,
    ctx: &Arc<TaskContext>,
) -> Result<PreparedPlan> {
    let worker_resolver = get_distributed_worker_resolver(ctx.session_config())?;
    let codec = DistributedCodec::new_combined_with_user(ctx.session_config());

    let urls = worker_resolver.get_urls()?;

    let metrics = CoordinatorToWorkerMetrics::new(metrics);

    let mut join_set = JoinSet::new();
    let prepared = Arc::clone(base_plan).transform_up(|plan| {
        // The following logic is just applied on network boundaries.
        let Some(plan) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };

        let Stage::Local(stage) = plan.input_stage() else {
            return exec_err!("Input stage from network boundary was not in Local state");
        };

        let mut spawner = CoordinatorToWorkerTaskSpawner::new(
            stage,
            &metrics,
            task_metrics,
            &codec,
            &mut join_set,
        )?;

        // Right now, we assign random workers to tasks. This might change in the future.
        let start_idx = rand::rng().random_range(0..urls.len());

        let mut workers = Vec::with_capacity(stage.tasks);
        for i in 0..stage.tasks {
            let url = urls[(start_idx + i) % urls.len()].clone();
            workers.push(url.clone());
            // Spawns the task that feeds this subplan to this worker. There will be as
            // many as this spawned tasks as workers.
            let (tx, worker_rx) = spawner.send_plan_task(Arc::clone(ctx), i, url)?;
            spawner.metrics_collection_task(i, worker_rx);
            spawner.work_unit_feed_task(Arc::clone(ctx), i, tx)?;
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
        join_set,
    })
}
