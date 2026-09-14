use crate::distributed_planner::DistributedConfig;
use crate::distributed_planner::distributed_query_planner::DistributedQueryPlanner;
use crate::events::{
    DesiredTaskCountHandlers, RandomRouteTaskHandler, RouteTaskHandlers, ScaleUpLeafNodeHandlers,
    SingleTaskChildUrlRouteTaskHandler, SingleTaskCoordinatorRouteTaskHandler,
    file_scan_config_desired_task_count, file_scan_config_scale_up_leaf_node,
};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// Extension trait for [SessionStateBuilder].
pub trait SessionStateBuilderExt {
    /// Injects a [QueryPlanner] implementation that attempts to distribute the plan after the
    /// normal planning passes are performed.
    ///
    /// It will wrap the existing query planner if one, so while setting up DataFusion's
    /// [SessionStateBuilder], install the custom user query planner with
    /// [SessionStateBuilder::with_query_planner] strictly *before* calling
    /// [SessionStateBuilderExt::with_distributed_planner].
    fn with_distributed_planner(self) -> Self;
}

impl SessionStateBuilderExt for SessionStateBuilder {
    fn with_distributed_planner(mut self) -> Self {
        let cfg = self.config().get_or_insert_default();
        inject_distributed_extensions(cfg);

        let prev = std::mem::take(self.query_planner());
        self.with_query_planner(Arc::new(DistributedQueryPlanner::new(prev)))
    }
}

/// Adds the configuration and built-in event handlers required by distributed planning.
///
/// This is the configuration half of [`SessionStateBuilderExt::with_distributed_planner`]. It is
/// exposed separately for integrations that provide their own [`QueryPlanner`] installation path,
/// such as an FFI binding.
pub fn inject_distributed_extensions(cfg: &mut SessionConfig) {
    DistributedConfig::ensure_in_config(cfg);
    cfg.options_mut()
        .optimizer
        .enable_physical_uncorrelated_scalar_subquery = false;

    DesiredTaskCountHandlers::push_builtin(cfg, Arc::new(file_scan_config_desired_task_count));
    ScaleUpLeafNodeHandlers::push_builtin(cfg, Arc::new(file_scan_config_scale_up_leaf_node));

    RouteTaskHandlers::extend_builtin(
        cfg,
        vec![
            Arc::new(SingleTaskCoordinatorRouteTaskHandler),
            Arc::new(SingleTaskChildUrlRouteTaskHandler),
            Arc::new(RandomRouteTaskHandler),
        ],
    );
}
