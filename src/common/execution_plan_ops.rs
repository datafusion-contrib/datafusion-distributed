use datafusion::common::plan_err;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub fn one_child(
    children: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    if children.len() != 1 {
        return plan_err!("Expected exactly 1 children, got {}", children.len());
    }
    Ok(children[0].clone())
}
