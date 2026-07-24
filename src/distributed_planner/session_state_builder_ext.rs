use crate::distributed_planner::DistributedConfig;
use crate::distributed_planner::distributed_query_planner::DistributedQueryPlanner;
use crate::events::{
    DesiredTaskCountHandlers, RouteTasksHandlers, ScaleUpLeafNodeHandlers,
    file_scan_config_desired_task_count, file_scan_config_scale_up_leaf_node, random_routing,
    single_task_child_url_routing, single_task_coordinator_routing,
};
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
        let cfg = self.config().get_or_insert_default();
        DistributedConfig::ensure_in_config(cfg);
        cfg.options_mut()
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery = false;

        // Add default event handlers for FileScanConfig nodes.
        DesiredTaskCountHandlers::push_builtin(cfg, Arc::new(file_scan_config_desired_task_count));
        ScaleUpLeafNodeHandlers::push_builtin(cfg, Arc::new(file_scan_config_scale_up_leaf_node));

        // Add default routing event handlers:
        RouteTasksHandlers::extend_builtin(
            cfg,
            vec![
                // 1. If there's a single task to route, plate it in the coordinator if it can also act as
                //    a worker.
                Arc::new(single_task_coordinator_routing),
                // 2. If there's a single task to route, and it cannot be placed in the coordinator,
                //    co-locate it in one of the workers from the stage below to avoid network transfers.
                Arc::new(single_task_child_url_routing),
                // 3. If everything above fails, just place randomly.
                Arc::new(random_routing),
            ],
        );

        let prev = std::mem::take(self.query_planner());
        self.with_query_planner(Arc::new(DistributedQueryPlanner { prev }))
    }
}
