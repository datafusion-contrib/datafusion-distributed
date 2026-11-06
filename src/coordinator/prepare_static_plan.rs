use crate::common::TreeNodeExt;
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::dynamic_filtering::{
    is_remote_dynamic_filtering_enabled, orphan_dynamic_filter_consumers,
};
use crate::stage::{LocalStage, RemoteStage};
use crate::{NetworkBoundaryExt, Stage, WorkerLocation};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, HashSet, Result, exec_datafusion_err};
use datafusion::physical_plan::ExecutionPlan;
use futures::FutureExt;
use futures::future::try_join_all;
use std::sync::Arc;
use tokio::sync::oneshot;

pub(super) async fn prepare_static_plan(
    query_coordinator: &Arc<QueryCoordinator>,
    base_plan: &Arc<dyn ExecutionPlan>,
) -> Result<PreparedPlan> {
    Ok(PreparedPlan {
        head_stage: prepare_stages(query_coordinator, base_plan, true).await?,
        plan_for_viz: Arc::clone(base_plan),
    })
}

/// Worker plans need concrete child destinations. Only inputs consumed directly by the
/// coordinator can remain pending, letting each task execute after its own setup succeeds.
pub(super) async fn prepare_stages(
    query_coordinator: &Arc<QueryCoordinator>,
    plan: &Arc<dyn ExecutionPlan>,
    defer_head: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut head_stages = HashSet::new();
    if defer_head {
        plan.apply(|plan| {
            if let Some(boundary) = plan.as_network_boundary() {
                head_stages.insert(boundary.input_stage().num());
                Ok(TreeNodeRecursion::Jump)
            } else {
                Ok(TreeNodeRecursion::Continue)
            }
        })?;
    }
    let prepared = Arc::clone(plan).transform_up_async(async |plan| {
        let Some(boundary) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };
        let Stage::Local(stage) = boundary.input_stage() else {
            return Ok(Transformed::no(plan));
        };
        let anchors = if is_remote_dynamic_filtering_enabled(query_coordinator.session_config())? {
            orphan_dynamic_filter_consumers(&stage.plan)?
        } else {
            vec![]
        };
        let workers = if head_stages.contains(&stage.num) {
            start_pending_stage(query_coordinator, stage)
        } else {
            let workers = try_join_all(
                (0..stage.tasks).map(|task| query_coordinator.start_task(stage, task)),
            )
            .await?
            .into_iter()
            .map(|(url, ..)| url.into())
            .collect();
            query_coordinator
                .stage_coordinator(stage)
                .seal_dynamic_filter_stage()?;
            workers
        };
        Ok(Transformed::yes(boundary.with_input_stage(
            Stage::Remote(RemoteStage {
                query_id: stage.query_id,
                num: stage.num,
                workers,
                runtime_stats: None,
                dynamic_filter_anchors: anchors,
            }),
        )?))
    });
    Ok(prepared.await?.data)
}

fn start_pending_stage(
    query_coordinator: &Arc<QueryCoordinator>,
    stage: &LocalStage,
) -> Vec<WorkerLocation> {
    let mut workers = Vec::with_capacity(stage.tasks);
    let mut setups = Vec::with_capacity(stage.tasks);
    for task in 0..stage.tasks {
        let (tx, rx) = oneshot::channel();
        workers.push(WorkerLocation::Pending {
            url: async move {
                rx.await
                    .map_err(|_| Arc::new(exec_datafusion_err!("Task setup was cancelled")))?
            }
            .boxed()
            .shared(),
            query_finished: query_coordinator.spawner.query_finished(),
        });
        setups.push((tx, query_coordinator.start_task(stage, task)));
    }
    let coordinator = Arc::clone(query_coordinator);
    let stage = stage.clone();
    let finished = query_coordinator.spawner.query_finished();
    query_coordinator.spawner.spawn(async move {
        let stage_coordinator = coordinator.stage_coordinator(&stage);
        let setup = async {
            try_join_all(setups.into_iter().map(|(tx, setup)| async move {
                let result = setup.await.map(|(url, ..)| url).map_err(Arc::new);
                let _ = tx.send(result.clone());
                result.map_err(DataFusionError::Shared)
            }))
            .await?;
            stage_coordinator.seal_dynamic_filter_stage()
        };
        tokio::select! {
            result = setup => result,
            _ = finished.cancelled() => Ok(()),
        }
    });
    workers
}
