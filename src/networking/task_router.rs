use crate::networking::worker_resolver::WorkerInfo;
use crate::stage::Stage;
use crate::{DistributedConfig, config_extension_ext::set_distributed_option_extension};
use datafusion::common::{DataFusionError, not_impl_err};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// Information available when assigning a distributed task to a worker.
pub struct TaskRouteRequest<'a> {
    /// The entire distributed query plan being executed.
    pub query_plan: &'a Arc<dyn ExecutionPlan>,
    /// The stage that is about to have workers assigned.
    pub stage: &'a Stage,
    /// The subplan that will be sent to the worker for this stage.
    pub stage_plan: &'a Arc<dyn ExecutionPlan>,
    /// The 0-based task number within the stage.
    pub task_number: usize,
    /// All workers currently available for routing.
    pub available_workers: &'a [WorkerInfo],
}

/// Allows users to choose which worker should execute a given distributed task.
pub trait TaskRouter {
    /// Returns the id of the worker that should execute the task.
    ///
    /// Returning `Ok(None)` defers to the default assignment strategy.
    fn route_task(&self, request: &TaskRouteRequest<'_>)
    -> Result<Option<String>, DataFusionError>;
}

pub(crate) fn set_distributed_task_router(
    cfg: &mut SessionConfig,
    router: impl TaskRouter + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    let task_router = TaskRouterExtension(Some(Arc::new(router)));
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_task_router = task_router;
    } else {
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_task_router: task_router,
                ..Default::default()
            },
        )
    }
}

pub fn get_distributed_task_router(
    cfg: &SessionConfig,
) -> Option<Arc<dyn TaskRouter + Send + Sync>> {
    let opts = cfg.options();
    let distributed_cfg = opts.extensions.get::<DistributedConfig>()?;
    distributed_cfg.__private_task_router.0.clone()
}

#[derive(Clone, Default)]
pub(crate) struct TaskRouterExtension(pub(crate) Option<Arc<dyn TaskRouter + Send + Sync>>);

impl TaskRouter for Arc<dyn TaskRouter + Send + Sync> {
    fn route_task(
        &self,
        request: &TaskRouteRequest<'_>,
    ) -> Result<Option<String>, DataFusionError> {
        self.as_ref().route_task(request)
    }
}

impl TaskRouterExtension {
    #[allow(dead_code)]
    pub(crate) fn not_implemented() -> Self {
        struct NotImplementedTaskRouter;

        impl TaskRouter for NotImplementedTaskRouter {
            fn route_task(
                &self,
                _: &TaskRouteRequest<'_>,
            ) -> Result<Option<String>, DataFusionError> {
                not_impl_err!("TaskRouter::route_task() not implemented")
            }
        }

        Self(Some(Arc::new(NotImplementedTaskRouter)))
    }
}
