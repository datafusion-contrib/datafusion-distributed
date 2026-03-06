use crate::distributed_planner::distributed_context::DistributedContext;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct EndDistributedContext;

impl PhysicalOptimizerRule for EndDistributedContext {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let d_ctx = DistributedContext::ensure(self, &plan)?;
        Ok(Arc::clone(&d_ctx.plan))
    }

    fn name(&self) -> &str {
        "EndDistributedContext"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
