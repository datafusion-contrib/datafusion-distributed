use crate::common::TreeNodeExt;
use crate::coordinator::distributed::PreparedPlan;
use crate::coordinator::query_coordinator::QueryCoordinator;
use crate::dynamic_filtering::{
    is_remote_dynamic_filtering_enabled, orphan_dynamic_filter_consumers,
};
use crate::stage::RemoteStage;
use crate::{NetworkBoundaryExt, Stage};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{HashSet, Result, exec_err};
use datafusion::physical_plan::ExecutionPlan;
use futures::future::try_join_all;
use std::sync::Arc;

pub(super) async fn prepare_static_plan(
    query_coordinator: &Arc<QueryCoordinator>,
    base_plan: &Arc<dyn ExecutionPlan>,
) -> Result<PreparedPlan> {
    let mut coordinator_input_stage_ids = HashSet::new();
    base_plan.apply(|plan| {
        if let Some(boundary) = plan.as_network_boundary() {
            coordinator_input_stage_ids.insert(boundary.input_stage().num());
            Ok(TreeNodeRecursion::Jump)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    let prepared = Arc::clone(base_plan).transform_up_async(async |plan| {
        let Some(boundary) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };
        let Stage::Local(stage) = boundary.input_stage() else {
            return exec_err!("Expected a local stage during static plan preparation");
        };
        let anchors = if is_remote_dynamic_filtering_enabled(query_coordinator.session_config())? {
            orphan_dynamic_filter_consumers(&stage.plan)?
        } else {
            vec![]
        };
        let workers = if coordinator_input_stage_ids.contains(&stage.num) {
            query_coordinator.start_pending_stage(stage)
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
    Ok(PreparedPlan {
        head_stage: prepared.await?.data,
        plan_for_viz: Arc::clone(base_plan),
    })
}
