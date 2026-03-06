use crate::distributed_planner::distributed_context::DistributedContext;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use std::sync::Arc;

#[derive(Debug)]
pub struct AddCoalesceOnTop;

impl PhysicalOptimizerRule for AddCoalesceOnTop {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let d_ctx = DistributedContext::ensure(self, &plan)?;

        if d_ctx.plan.output_partitioning().partition_count() > 1 {
            let child = Arc::clone(&d_ctx.plan);
            return plan.with_new_children(vec![Arc::new(CoalescePartitionsExec::new(child))]);
        }
        Ok(plan)
    }

    fn name(&self) -> &str {
        "AddCoalesceOnTop"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
