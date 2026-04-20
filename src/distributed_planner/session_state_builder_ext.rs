use crate::DistributedExec;
use crate::distributed_planner::distribute_plan::distribute_plan;
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use std::sync::Arc;

/// Extension trait for [SessionStateBuilder].
pub trait SessionStateBuilderExt {
    /// Injects a [QueryPlanner] implementation that attempts to distribute the plan after the
    /// normal planning passes are performed.
    ///
    /// It will wrap the existing query planner if one, so while setting up DataFusion's
    /// [SessionStateBuilder], it's important to inject the custom user query planner implementation
    /// with [SessionStateBuilderExt::with_distributed_planner] strictly *before* calling
    /// [SessionStateBuilder::with_query_planner].
    fn with_distributed_planner(self) -> Self;
}

impl SessionStateBuilderExt for SessionStateBuilder {
    fn with_distributed_planner(mut self) -> Self {
        let prev = std::mem::take(self.query_planner());
        self.with_query_planner(Arc::new(DistributedQueryPlanner { prev }))
    }
}
#[derive(Debug)]
struct DistributedQueryPlanner {
    prev: Option<Arc<dyn QueryPlanner + Send + Sync>>,
}

#[async_trait]
impl QueryPlanner for DistributedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let s_plan = match &self.prev {
            None => {
                // Use the default physical planner.
                let planner = DefaultPhysicalPlanner::default();
                planner
                    .create_physical_plan(logical_plan, session_state)
                    .await?
            }
            Some(prev) => {
                prev.create_physical_plan(logical_plan, session_state)
                    .await?
            }
        };
        match distribute_plan(Arc::clone(&s_plan), session_state.config_options()).await? {
            Some(d_plan) => Ok(Arc::new(DistributedExec::new(d_plan))),
            None => Ok(s_plan),
        }
    }
}
