use crate::distributed_planner::distributed_context::DistributedContext;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct StartDistributedContext;

impl PhysicalOptimizerRule for StartDistributedContext {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if plan.as_any().is::<DistributedContext>() {
            return Ok(plan);
        }
        Ok(Arc::new(DistributedContext::new(plan)))
    }

    fn name(&self) -> &str {
        "StartDistributedContext"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
