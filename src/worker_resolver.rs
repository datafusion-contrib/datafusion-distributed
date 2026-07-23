use crate::DistributedConfig;
use datafusion::common::{DataFusionError, exec_err, not_impl_err};
use datafusion::prelude::SessionConfig;
use std::any::Any;
use std::sync::Arc;
use url::Url;

/// Resolves a list of worker URLs in the cluster available for executing parts of the plan.
pub trait WorkerResolver: Any + Send + Sync {
    /// Gets all available worker URLs in the cluster. Note how this method is not async, which
    /// means that any async operation involved in discovering worker URLs must happen on a
    /// background thread and be retrieved by this method synchronously.
    ///
    /// This method will be called in several places during distributed planning:
    /// - During task count assignation for the different stages, for determining the size of
    ///   the cluster and limiting the amount of tasks per stage to Vec<Url>.length().
    /// - Right before execution, for lazily assigning worker URLs to the different tasks in the
    ///   plan. This is done as close to execution in order to have fresh worker URLs as updated
    ///   as possible.
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
}

pub(crate) fn set_distributed_worker_resolver(
    cfg: &mut SessionConfig,
    worker_resolver: impl WorkerResolver + 'static,
) {
    DistributedConfig::ensure_in_config(cfg);
    cfg.set_extension(Arc::new(WorkerResolverExtension(Arc::new(worker_resolver))));
}

/// Gets the [WorkerResolver] from the [SessionConfig]'s extensions. Typically called inside
/// [RouteTasksHandler::route_tasks] to resolve the worker URLs available for distributed tasks.
pub fn get_distributed_worker_resolver(
    cfg: &SessionConfig,
) -> Result<Arc<dyn WorkerResolver>, DataFusionError> {
    let Some(ext) = cfg.get_extension::<WorkerResolverExtension>() else {
        return exec_err!("WorkerResolver not present in the session config");
    };
    Ok(Arc::clone(&ext.0))
}

#[derive(Clone)]
pub(crate) struct WorkerResolverExtension(pub(crate) Arc<dyn WorkerResolver + 'static>);

impl WorkerResolverExtension {
    pub(crate) fn not_implemented() -> Self {
        struct NotImplementedWorkerResolver;
        impl WorkerResolver for NotImplementedWorkerResolver {
            fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
                not_impl_err!("WorkerResolver::get_urls() not implemented")
            }
        }
        Self(Arc::new(NotImplementedWorkerResolver))
    }

    pub(crate) fn from_session_config(cfg: &SessionConfig) -> Arc<Self> {
        cfg.get_extension::<WorkerResolverExtension>()
            .unwrap_or_else(|| Arc::new(Self::not_implemented()))
    }
}

impl WorkerResolver for Arc<dyn WorkerResolver> {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.as_ref().get_urls()
    }
}
