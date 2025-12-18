use datafusion::common::{DataFusionError, plan_err};
use datafusion::physical_plan::ExecutionPlan;
use std::borrow::Borrow;
use std::sync::Arc;

pub(crate) fn require_one_child<L, T>(
    children: L,
) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>
where
    L: AsRef<[T]>,
    T: Borrow<Arc<dyn ExecutionPlan>>,
{
    let children = children.as_ref();
    if children.len() != 1 {
        return plan_err!("Expected exactly 1 children, got {}", children.len());
    }
    Ok(children[0].borrow().clone())
}
