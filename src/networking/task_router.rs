use crate::{DistributedConfig, config_extension_ext::set_distributed_option_extension};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use url::Url;

/// Allows users to choose which worker should execute a given distributed task.
pub trait TaskRouter {
    /// Returns the url of the worker that should execute the task.
    fn route_fn(&self, start_index: usize, task_number: usize, num_urls: usize) -> usize {
        (start_index + task_number) % num_urls
    }

    /// Returns the url of the worker that should execute the task.
    fn route_task(&self, start_index: usize, task_number: usize, urls: &[Url]) -> Url {
        urls[self.route_fn(start_index, task_number, urls.len())].clone()
    }
}

struct DefaultTaskRouter;

impl TaskRouter for DefaultTaskRouter {}

pub(crate) fn set_distributed_task_router(
    cfg: &mut SessionConfig,
    router: impl TaskRouter + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    let task_router = TaskRouterExtension(Arc::new(router));
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

pub fn get_distributed_task_router(cfg: &SessionConfig) -> Arc<dyn TaskRouter + Send + Sync> {
    let opts = cfg.options();
    opts.extensions
        .get::<DistributedConfig>()
        .map(|distributed_cfg| Arc::clone(&distributed_cfg.__private_task_router.0))
        .unwrap_or_else(|| Arc::new(DefaultTaskRouter))
}

#[derive(Clone)]
pub(crate) struct TaskRouterExtension(pub(crate) Arc<dyn TaskRouter + Send + Sync>);

impl Default for TaskRouterExtension {
    fn default() -> Self {
        Self(Arc::new(DefaultTaskRouter))
    }
}
