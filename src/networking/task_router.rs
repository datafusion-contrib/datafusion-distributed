use crate::{DistributedConfig, config_extension_ext::set_distributed_option_extension};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use url::Url;

pub struct RouterInfo {
    pub task_number: usize,
    pub stage_seed: usize,
    pub urls: Vec<Url>,
    pub affinity_key: Option<Vec<u8>>,
}

/// Allows users to consistently route tasks to URLs. Defining a custom routing function may be
/// useful for users that want to ensure that e.g. repeated file reads are directed to the same
/// physical machines to allow for proper caching rather than repeatedly reading from object storage.
///
/// To implement this, include routing information as bytes in `affinity_key`. These bytes can
/// then be hashed to consistently point to a specific URL.
pub trait TaskRouter {
    /// Returns the url of the worker that should execute the task. The default implementation
    /// assigns tasks to workers round-robin starting from stage_seed.
    fn route(&self, router_info: RouterInfo) -> Url {
        router_info.urls
            [(router_info.stage_seed + router_info.task_number) % router_info.urls.len()]
        .clone()
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
