use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use datafusion::common::{DataFusionError, exec_err, not_impl_err};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use url::Url;

/// Resolves a list of worker URLs in the cluster available for executing parts of the plan.
pub trait WorkerResolver {
    /// Gets all available worker URLs. Used during task assignment.
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
}

pub(crate) fn set_distributed_worker_resolver(
    cfg: &mut SessionConfig,
    worker_resolver: impl WorkerResolver + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    let worker_resolver = WorkerResolverExtension::new(worker_resolver);
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_worker_resolver = worker_resolver;
    } else {
        set_distributed_option_extension(cfg, DistributedConfig {
            __private_worker_resolver: worker_resolver,
            ..Default::default()
        }).expect("Calling set_distributed_option_extension with a default DistributedConfig should never fail");
    }
}

pub(crate) fn get_distributed_worker_resolver(
    cfg: &SessionConfig,
) -> Result<&WorkerResolverExtension, DataFusionError> {
    let opts = cfg.options();
    let Some(distributed_cfg) = opts.extensions.get::<DistributedConfig>() else {
        return exec_err!("ChannelResolver not present in the session config");
    };
    Ok(&distributed_cfg.__private_worker_resolver)
}

#[derive(Clone)]
pub(crate) struct WorkerResolverExtension {
    inner: Arc<dyn WorkerResolver + Send + Sync + 'static>,
}

impl WorkerResolverExtension {
    pub(crate) fn new(inner: impl WorkerResolver + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn not_implemented() -> Self {
        struct NotImplementedWorkerResolver;
        impl WorkerResolver for NotImplementedWorkerResolver {
            fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
                not_impl_err!("WorkerResolver::get_urls() not implemented")
            }
        }
        Self::new(NotImplementedWorkerResolver)
    }

    pub(crate) fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.inner.get_urls()
    }
}
