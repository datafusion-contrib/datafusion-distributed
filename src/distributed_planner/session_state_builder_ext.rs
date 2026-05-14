use crate::distributed_planner::distributed_query_planner::DistributedQueryPlanner;
use datafusion::execution::SessionStateBuilder;
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
