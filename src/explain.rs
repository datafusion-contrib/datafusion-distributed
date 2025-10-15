use crate::display_plan_ascii;
use crate::execution_plans::DistributedExec;
use crate::metrics::rewrite_distributed_plan_with_metrics;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use std::sync::Arc;

/// explain_analyze renders an [ExecutionPlan] with metrics.
pub fn explain_analyze(executed: Arc<dyn ExecutionPlan>) -> Result<String, DataFusionError> {
    match executed.as_any().downcast_ref::<DistributedExec>() {
        None => Ok(DisplayableExecutionPlan::with_metrics(executed.as_ref())
            .indent(true)
            .to_string()),
        Some(_) => {
            let executed = rewrite_distributed_plan_with_metrics(executed.clone())?;
            Ok(display_plan_ascii(executed.as_ref()))
        }
    }
}
