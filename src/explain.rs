use crate::execution_plans::{DisplayCtx, StageExec};
use crate::metrics::MetricsCollectorResult;
use crate::metrics::TaskMetricsCollector;
use crate::metrics::proto::MetricsSetProto;
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::protobuf::StageKey;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use std::sync::Arc;

/// Rewriter that injects the display context into the [StageExec] nodes in the plan.
pub struct DisplayCtxReWriter {
    display_ctx: DisplayCtx,
}

impl DisplayCtxReWriter {
    pub fn new(display_ctx: DisplayCtx) -> Self {
        Self { display_ctx }
    }

    /// Rewrites the plan to inject the display context into the [StageExec] nodes in the plan.
    pub fn rewrite(
        mut self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let transformed = plan.rewrite(&mut self)?;
        Ok(transformed.data)
    }
}

impl TreeNodeRewriter for DisplayCtxReWriter {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>, DataFusionError> {
        match plan.as_any().downcast_ref::<StageExec>() {
            Some(stage_exec) => {
                let mut copy = stage_exec.clone();
                copy.display_ctx = Some(self.display_ctx.clone());
                Ok(Transformed::new(
                    Arc::new(copy),
                    true,
                    TreeNodeRecursion::Continue,
                ))
            }
            None => Err(DataFusionError::Internal("expected stage exec".to_string())),
        }
    }
}

pub fn explain_analyze(executed: Arc<dyn ExecutionPlan>) -> Result<String, DataFusionError> {
    // Check if the plan is distributed by looking for a root [StageExec].
    let plan = match executed.as_any().downcast_ref::<StageExec>() {
        None => executed,
        Some(stage_exec) => {
            // If the plan was distributed, collect metrics from the coordinating stage exec.
            let MetricsCollectorResult {
                task_metrics,
                mut input_task_metrics,
            } = TaskMetricsCollector::new().collect(stage_exec.plan.clone())?;
            input_task_metrics.insert(
                StageKey {
                    query_id: stage_exec.query_id.to_string(),
                    stage_id: stage_exec.num as u64,
                    task_number: 0,
                },
                task_metrics
                    .into_iter()
                    .map(|metrics| df_metrics_set_to_proto(&metrics))
                    .collect::<Result<Vec<MetricsSetProto>, DataFusionError>>()?,
            );

            let display_ctx = DisplayCtx::new(input_task_metrics);
            // Inject the display context into the plan.
            DisplayCtxReWriter::new(display_ctx).rewrite(executed.clone())?
        }
    };

    Ok(DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string())
}
