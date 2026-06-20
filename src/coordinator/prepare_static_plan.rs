use crate::coordinator::MetricsStore;
use crate::coordinator::distributed::PreparedPlan;
use crate::networking::get_distributed_worker_resolver;
use crate::stage::RemoteStage;
use crate::worker::{WorkerDispatch, WorkerDispatchRequest};
use crate::{DistributedConfig, NetworkBoundaryExt, Stage, TaskEstimator, TaskRoutingContext};
use datafusion::common::runtime::JoinSet;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, exec_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::ExecutionPlan;
use rand::Rng;
use std::sync::Arc;
use url::Url;

/// Prepares the distributed plan for execution, which implies:
/// 1. Perform some worker URL assignation, choosing either:
///    - The URLs set by the user with [crate::TaskEstimator::route_tasks].
///    - Randomly otherwise
/// 2. Hand each network boundary's stage to the [WorkerDispatch], which delivers the sliced
///    subplans to the assigned workers and wires up whatever back-channels the transport needs.
/// 3. In each network boundary, set the input stage to its `Remote` form holding the assigned
///    worker URLs. Traversing them will not go further down, as they become leaf-like.
pub(super) fn prepare_static_plan(
    base_plan: &Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
    dispatcher: &dyn WorkerDispatch,
    join_set: &mut JoinSet<Result<()>>,
    metrics: &ExecutionPlanMetricsSet,
    metrics_store: Option<&Arc<MetricsStore>>,
) -> Result<PreparedPlan> {
    let prepared = Arc::clone(base_plan).transform_up(|plan| {
        // The following logic is just applied on network boundaries.
        let Some(plan) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };

        let Stage::Local(stage) = plan.input_stage() else {
            return exec_err!("Input stage from network boundary was not in Local state");
        };

        let routed_urls = routed_urls(task_ctx, &stage.plan, stage.tasks)?;

        dispatcher.dispatch(WorkerDispatchRequest {
            stage,
            routed_urls: &routed_urls,
            task_ctx,
            metrics,
            metrics_store,
            join_set,
        })?;

        Ok(Transformed::yes(plan.with_input_stage(Stage::Remote(
            RemoteStage {
                query_id: stage.query_id,
                num: stage.num,
                workers: routed_urls,
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

/// Returns as many URLs as the `task_count` for the stage. These URLs can be:
/// - chosen by the user, if they provided an implementation for [TaskEstimator::route_tasks].
/// - assigned via round-robin from a randomized starting point otherwise.
fn routed_urls(
    task_ctx: &Arc<TaskContext>,
    plan: &Arc<dyn ExecutionPlan>,
    task_count: usize,
) -> Result<Vec<Url>> {
    let session_config = task_ctx.session_config();
    let d_cfg = DistributedConfig::from_config_options(session_config.options())?;
    let worker_resolver = get_distributed_worker_resolver(session_config)?;
    let task_estimator = &d_cfg.__private_task_estimator;

    let routed_urls = match task_estimator.route_tasks(&TaskRoutingContext {
        task_ctx: Arc::clone(task_ctx),
        plan,
        task_count,
    }) {
        Ok(Some(routed_urls)) => routed_urls,
        Ok(None) => {
            let available_urls = worker_resolver.get_urls()?;
            let start_idx = rand::rng().random_range(0..available_urls.len());
            (0..task_count)
                .map(|i| available_urls[(start_idx + i) % available_urls.len()].clone())
                .collect()
        }
        Err(e) => return exec_err!("error routing tasks to workers: {e}"),
    };

    if routed_urls.len() != task_count {
        return exec_err!(
            "number of tasks ({}) was not equal to number of urls ({}) at execution time",
            task_count,
            routed_urls.len()
        );
    }
    Ok(routed_urls)
}
