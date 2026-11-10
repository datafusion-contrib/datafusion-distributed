use crate::WorkerChannel;
use crate::distributed_planner::DistributedConfig;
#[cfg(feature = "grpc")]
use crate::protocol::grpc;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use url::Url;

/// Allows users to customize the way Worker clients are created. A common use case is to
/// wrap the client with tower layers or schedule it in an IO-specific tokio runtime.
///
/// There is a default implementation of this trait that should be enough for the most common
/// use-cases.
///
/// # Implementation Notes
/// - This is called per request, so implementors of this trait should make sure that
///   clients are reused across method calls instead of building a new Worker client every time.
///
/// - When implementing `get_worker_client_for_url`, it is recommended to use the
///   [`create_worker_client`] helper function to ensure clients are configured with
///   appropriate message size limits for internal communication. This helps avoid message
///   size errors when transferring large datasets.
#[async_trait]
pub trait ChannelResolver {
    /// For a given URL, get a Worker gRPC client for communicating to it.
    ///
    /// *WARNING*: This method is called for every gRPC request, so to not create
    /// one client connection for each request, users are required to reuse generated clients.
    /// It's recommended to rely on [DefaultChannelResolver] either by delegating method calls
    /// to it or by copying the implementation.
    ///
    /// Consider using [`create_worker_client`] to create the client with appropriate
    /// default message size limits.
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<Box<dyn WorkerChannel>, DataFusionError>;
}

pub(crate) fn set_distributed_channel_resolver(
    cfg: &mut SessionConfig,
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
) {
    cfg.set_extension(Arc::new(ChannelResolverExtension(Some(Arc::new(
        channel_resolver,
    )))));
    DistributedConfig::ensure_in_config(cfg);
}

pub fn get_distributed_channel_resolver(
    task_ctx: &TaskContext,
) -> Arc<dyn ChannelResolver + Send + Sync> {
    let session_cfg = task_ctx.session_config();
    if let Some(channel_resolver_ext) = session_cfg.get_extension::<ChannelResolverExtension>()
        && let Some(cr) = &channel_resolver_ext.0
    {
        return Arc::clone(cr);
    }

    #[cfg(feature = "grpc")]
    {
        let runtime_addr = Arc::as_ptr(&task_ctx.runtime_env()) as usize;
        grpc::DEFAULT_CHANNEL_RESOLVER_PER_RUNTIME.get_with(runtime_addr, || {
            Arc::new(grpc::DefaultChannelResolver::default())
        })
    }

    #[cfg(not(feature = "grpc"))]
    {
        panic!(
            "gRPC feature is not enabled, and no channel resolver was provided, so no default ChannelResolver can be provided"
        );
    }
}

#[async_trait]
impl ChannelResolver for Arc<dyn ChannelResolver + Send + Sync> {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<Box<dyn WorkerChannel>, DataFusionError> {
        self.as_ref().get_worker_client_for_url(url).await
    }
}

#[derive(Clone, Default)]
pub(crate) struct ChannelResolverExtension(Option<Arc<dyn ChannelResolver + Send + Sync>>);
