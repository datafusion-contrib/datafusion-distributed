use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use datafusion::common::{DataFusionError, exec_err, not_impl_err};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use url::Url;

/// Resolves a list of worker URLs in the cluster available for executing parts of the plan.
pub trait WorkerResolver {
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
    worker_resolver: impl WorkerResolver + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    let worker_resolver = WorkerResolverExtension(Arc::new(worker_resolver));
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_worker_resolver = worker_resolver;
    } else {
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_worker_resolver: worker_resolver,
                ..Default::default()
            },
        )
    }
}

pub fn get_distributed_worker_resolver(
    cfg: &SessionConfig,
) -> Result<Arc<dyn WorkerResolver + Send + Sync>, DataFusionError> {
    let opts = cfg.options();
    let Some(distributed_cfg) = opts.extensions.get::<DistributedConfig>() else {
        return exec_err!("ChannelResolver not present in the session config");
    };
    Ok(Arc::clone(&distributed_cfg.__private_worker_resolver.0))
}

#[derive(Clone)]
pub(crate) struct WorkerResolverExtension(
    pub(crate) Arc<dyn WorkerResolver + Send + Sync + 'static>,
);

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
}

impl WorkerResolver for Arc<dyn WorkerResolver + Send + Sync> {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.as_ref().get_urls()
    }
}
