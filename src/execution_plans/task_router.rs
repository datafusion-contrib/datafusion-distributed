use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use datafusion::common::Result;
use datafusion::common::not_impl_err;
use datafusion::config::{ConfigField, ConfigOptions, Visit};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use url::Url;

/// The result of routing a single distributed task: the worker URL to send to and the plan to
/// send.
pub struct TaskAssignment {
    /// The URL of the worker to which this task's plan should be sent.
    pub url: Url,
    /// The plan to send to the worker, potentially specialized for this task (e.g. with
    /// non-owned file groups emptied).
    pub plan: Arc<dyn ExecutionPlan>,
}

/// Determines, for each task, which worker URL to route to and what plan to send.
///
/// The coordinator calls [`TaskRouter::route`] once per task immediately before serializing
/// and dispatching the plan. Implementations can use this single hook to:
/// - Pick a URL via consistent hashing for data-locality-aware routing.
/// - Specialize the plan to remove partitions the target worker will not access.
///
/// Worker URLs are supplied by the framework from [`WorkerResolver::get_urls`]; the router
/// does not perform its own URL discovery.
pub trait TaskRouter: Debug + Send + Sync {
    /// Given the stage plan, task index, total task count, and the full list of available
    /// worker URLs, return the URL to route to and the plan to send to that worker.
    fn route(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        task_index: usize,
        task_count: usize,
        available_urls: &[Url],
    ) -> Result<TaskAssignment>;
}

/// Default [`TaskRouter`] that round-robins tasks across available workers without modifying
/// the plan. Preserves the pre-existing assignment behaviour.
#[derive(Debug, Default)]
pub(crate) struct RoundRobinTaskRouter;

impl TaskRouter for RoundRobinTaskRouter {
    fn route(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        task_index: usize,
        _task_count: usize,
        available_urls: &[Url],
    ) -> Result<TaskAssignment> {
        let url = available_urls[task_index % available_urls.len()].clone();
        Ok(TaskAssignment { url, plan })
    }
}

/// Wrapper that lets [`TaskRouter`] live inside [`DistributedConfig`] via the
/// `extensions_options!` macro.
#[derive(Clone)]
pub(crate) struct TaskRouterExtension(pub(crate) Arc<dyn TaskRouter>);

impl Default for TaskRouterExtension {
    fn default() -> Self {
        Self(Arc::new(RoundRobinTaskRouter))
    }
}

impl ConfigField for TaskRouterExtension {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {}

    fn set(&mut self, _: &str, _: &str) -> Result<()> {
        not_impl_err!("TaskRouterExtension cannot be set via string")
    }
}

impl Debug for TaskRouterExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskRouterExtension")
    }
}

/// Returns the registered [`TaskRouter`], falling back to [`RoundRobinTaskRouter`] if none
/// has been set.
pub(crate) fn get_task_router(cfg: &ConfigOptions) -> Arc<dyn TaskRouter> {
    cfg.extensions
        .get::<DistributedConfig>()
        .map(|c| Arc::clone(&c.__private_task_router.0))
        .unwrap_or_else(|| Arc::new(RoundRobinTaskRouter))
}

/// Registers a [`TaskRouter`] in the session config.
pub(crate) fn set_task_router(cfg: &mut SessionConfig, router: impl TaskRouter + 'static) {
    set_task_router_arc(cfg, Arc::new(router))
}

/// Registers a [`TaskRouter`] in the session config from an `Arc`.
pub(crate) fn set_task_router_arc(cfg: &mut SessionConfig, router: Arc<dyn TaskRouter>) {
    let extension = TaskRouterExtension(router);
    let opts = cfg.options_mut();
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_task_router = extension;
    } else {
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_task_router: extension,
                ..Default::default()
            },
        )
    }
}
