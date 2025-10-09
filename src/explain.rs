use crate::display_plan_ascii;
use crate::execution_plans::DistributedExec;
use crate::metrics::MetricsCollectorResult;
use crate::metrics::TaskMetricsCollector;
use crate::metrics::proto::MetricsSetProto;
use crate::metrics::proto::df_metrics_set_to_proto;
use crate::stage::DisplayCtx;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use std::sync::Arc;

/// explain_analyze renders an [ExecutionPlan] with metrics.
pub fn explain_analyze(executed: Arc<dyn ExecutionPlan>) -> Result<String, DataFusionError> {
    // Check if the plan is distributed by looking for a root DistributedExec.
    match executed.as_any().downcast_ref::<DistributedExec>() {
        None => Ok(DisplayableExecutionPlan::with_metrics(executed.as_ref())
            .indent(true)
            .to_string()),
        Some(dist_exec) => {
            // If the plan was distributed, collect metrics from the coordinating stage exec.
            // TODO: Should we move this into the DistributedExec itself or a new ExplainAnalyzeExec?
            let MetricsCollectorResult {
                task_metrics,
                mut input_task_metrics,
            } = TaskMetricsCollector::new().collect(dist_exec.pepared_plan()?)?;

            input_task_metrics.insert(
                dist_exec.to_stage_key(),
                task_metrics
                    .into_iter()
                    .map(|metrics| df_metrics_set_to_proto(&metrics))
                    .collect::<Result<Vec<MetricsSetProto>, DataFusionError>>()?,
            );

            let display_ctx = DisplayCtx::new(input_task_metrics);
            Ok(display_plan_ascii(&dist_exec.with_display_ctx(display_ctx)))
        }
    }
}
