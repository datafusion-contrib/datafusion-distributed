use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, config_datafusion_err, exec_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use futures::FutureExt;
use futures::future::Shared;
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tonic::body::Body;
use tonic::codegen::BoxFuture;
use tonic::transport::Channel;
use tower::ServiceExt;
use url::Url;

/// Allows users to customize the way Arrow Flight clients are created. A common use case is to
/// wrap the client with tower layers or schedule it in an IO-specific tokio runtime.
///
/// There is a default implementation of this trait that should be enough for the most common
/// use-cases.
///
/// # Implementation Notes
/// - This is called per Arrow Flight request, so implementors of this trait should make sure that
///   clients are reused across method calls instead of building a new Arrow Flight client
///   every time.
///
/// - When implementing `get_flight_client_for_url`, it is recommended to use the
///   [`create_flight_client`] helper function to ensure clients are configured with
///   appropriate message size limits for internal communication. This helps avoid message
///   size errors when transferring large datasets.
#[async_trait]
pub trait ChannelResolver {
    /// For a given URL, get an Arrow Flight client for communicating to it.
    ///
    /// *WARNING*: This method is called for every Arrow Flight gRPC request, so to not create
    /// one client connection for each request, users are required to reuse generated clients.
    /// It's recommended to relly on [DefaultChannelResolver] either by delegating method calls
    /// to it or by copying the implementation.
    ///
    /// Consider using [`create_flight_client`] to create the client with appropriate
    /// default message size limits.
    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError>;
}

pub(crate) fn set_distributed_channel_resolver(
    cfg: &mut SessionConfig,
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    let channel_resolver = ChannelResolverExtension(Some(Arc::new(channel_resolver)));
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_channel_resolver = channel_resolver;
    } else {
        set_distributed_option_extension(cfg, DistributedConfig {
            __private_channel_resolver: channel_resolver,
            ..Default::default()
        }).expect("Calling set_distributed_option_extension with a default DistributedConfig should never fail");
    }
}

// Unlike TaskContext, a DataFusion RuntimeEnv does not allow to introduce user-defined extensions.
// For the default implementation of the ChannelResolvers, we cannot inject one DefaultChannelResolver
// per TaskContext, as this holds reference to Tonic channels that must outlive a single TaskContext.
//
// The Tonic channels need to be established and reused under a whole RuntimeEnv scope, not a single
// TaskContext, which forces us to put the default implementation in a static global variable that
// stores and reuses tonic channels per RuntimeEnv's pointer address.
static DEFAULT_CHANNEL_RESOLVER_PER_RUNTIME: LazyLock<
    moka::sync::Cache<
        /* Arc<RuntimeEnv> pointer address */ usize,
        /* ChannelResolver that reuses built channels */ Arc<DefaultChannelResolver>,
    >,
> = LazyLock::new(|| moka::sync::Cache::builder().max_capacity(256).build());

pub(crate) fn get_distributed_channel_resolver(
    task_ctx: &TaskContext,
) -> Arc<dyn ChannelResolver + Send + Sync> {
    let opts = task_ctx.session_config().options();
    if let Some(distributed_cfg) = opts.extensions.get::<DistributedConfig>() {
        if let Some(cr) = &distributed_cfg.__private_channel_resolver.0 {
            return Arc::clone(cr);
        }
    }
    let runtime_addr = Arc::as_ptr(&task_ctx.runtime_env()) as usize;
    DEFAULT_CHANNEL_RESOLVER_PER_RUNTIME
        .get_with(runtime_addr, || Arc::new(DefaultChannelResolver::default()))
}

pub type BoxCloneSyncChannel = tower::util::BoxCloneSyncService<
    http::Request<Body>,
    http::Response<Body>,
    tonic::transport::Error,
>;

type ChannelCacheValue = Shared<BoxFuture<BoxCloneSyncChannel, Arc<DataFusionError>>>;

#[derive(Clone, Default)]
pub(crate) struct ChannelResolverExtension(Option<Arc<dyn ChannelResolver + Send + Sync>>);

/// Default implementation of a [ChannelResolver] that connects to the workers given the URL once
/// and stores the connection instance in a TTI cache.
///
/// Sane default over which other [ChannelResolver] can be built for better customization of the
/// [FlightServiceClient]s.
#[derive(Clone)]
pub struct DefaultChannelResolver {
    cache: Arc<moka::sync::Cache<Url, ChannelCacheValue>>,
}

impl Default for DefaultChannelResolver {
    fn default() -> Self {
        Self {
            cache: Arc::new(
                moka::sync::Cache::builder()
                    // Use an unrealistic max capacity, just in case there is a logic error on the
                    // user part that produces an unreasonable amount of URLs.
                    .max_capacity(64556)
                    // If a channel has not been used in 5 mins, delete it.
                    .time_to_idle(Duration::from_secs(5 * 60))
                    .build(),
            ),
        }
    }
}

impl DefaultChannelResolver {
    /// Gets the cached [BoxCloneSyncChannel] for the given URL, or builds a new one.
    pub async fn get_channel(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError> {
        let channel = self.cache.get_with_by_ref(url, move || {
            let url = url.to_string();
            async move {
                let endpoint = Channel::from_shared(url.clone()).map_err(|err| {
                    config_datafusion_err!(
                        "Invalid URL '{url}' returned by WorkerResolver implementation: {err}"
                    )
                })?;
                let mut channel = endpoint.connect().await.map_err(|err| {
                    DataFusionError::Context(
                        format!("{err:?}"),
                        Box::new(exec_datafusion_err!(
                            "Error connecting to Distributed DataFusion worker on '{url}': {err}"
                        )),
                    )
                })?;
                channel.ready().await.map_err(|err| {
                    DataFusionError::Context(
                        format!("{err:?}"),
                        Box::new(exec_datafusion_err!(
                            "Error waiting for Distributed DataFusion channel to be ready on '{url}': {err}"
                        )),
                    )
                })?;
                Ok(BoxCloneSyncChannel::new(channel))
            }
            .boxed()
            .shared()
        });

        channel.await.map_err(|err| {
            self.cache.invalidate(url);
            DataFusionError::Shared(err)
        })
    }
}

#[async_trait]
impl ChannelResolver for DefaultChannelResolver {
    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        self.get_channel(url).await.map(create_flight_client)
    }
}

#[async_trait]
impl ChannelResolver for Arc<dyn ChannelResolver + Send + Sync> {
    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        self.as_ref().get_flight_client_for_url(url).await
    }
}

/// Creates a [`FlightServiceClient`] with high default message size limits.
///
/// This is a convenience function that wraps [`FlightServiceClient::new`] and configures
/// it with `max_decoding_message_size(usize::MAX)` and `max_encoding_message_size(usize::MAX)`
/// to avoid message size limitations for internal communication.
///
/// Users implementing custom [`ChannelResolver`]s should use this function in their
/// `get_flight_client_for_url` implementations to ensure consistent behavior with built-in
/// implementations.
///
/// # Example
///
/// ```rust,ignore
/// use datafusion_distributed::{create_flight_client, BoxCloneSyncChannel, ChannelResolver};
/// use arrow_flight::flight_service_client::FlightServiceClient;
/// use tonic::transport::Channel;
///
/// #[async_trait]
/// impl ChannelResolver for MyResolver {
///     async fn get_flight_client_for_url(
///         &self,
///         url: &Url,
///     ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
///         let channel = Channel::from_shared(url.to_string())?.connect().await?;
///         Ok(create_flight_client(BoxCloneSyncChannel::new(channel)))
///     }
/// }
/// ```
pub fn create_flight_client(
    channel: BoxCloneSyncChannel,
) -> FlightServiceClient<BoxCloneSyncChannel> {
    FlightServiceClient::new(channel)
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DefaultSessionBuilder;
    use crate::test_utils::localhost::spawn_flight_service;
    use datafusion::common::assert_contains;
    use datafusion::common::runtime::SpawnedTask;
    use std::error::Error;
    use std::time::Instant;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn fails_establishing_connection() -> Result<(), Box<dyn Error>> {
        let (url, _guard) = spawn_http_localhost_worker().await?;
        drop(_guard);
        let channel_resolver = DefaultChannelResolver::default();
        let err = channel_resolver.get_channel(&url).await.unwrap_err();
        assert_contains!(err.to_string(), "tcp connect error");
        Ok(())
    }

    #[tokio::test]
    async fn can_establish_connection() -> Result<(), Box<dyn Error>> {
        let (url, _guard) = spawn_http_localhost_worker().await?;
        let channel_resolver = DefaultChannelResolver::default();
        channel_resolver.get_channel(&url).await?;
        Ok(())
    }

    #[tokio::test]
    async fn channel_resolve_is_cached() -> Result<(), Box<dyn Error>> {
        let (url, _guard) = spawn_http_localhost_worker().await?;
        let channel_resolver = DefaultChannelResolver::default();

        let start = Instant::now();
        channel_resolver.get_channel(&url).await?;
        let first_call = start.elapsed();

        let start = Instant::now();
        channel_resolver.get_channel(&url).await?;
        let second_call = start.elapsed();

        assert!(first_call > second_call);
        Ok(())
    }

    async fn spawn_http_localhost_worker() -> Result<(Url, SpawnedTask<()>), Box<dyn Error>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;

        let port = listener
            .local_addr()
            .expect("Failed to get local address")
            .port();

        let task = SpawnedTask::spawn(async {
            if let Err(err) = spawn_flight_service(DefaultSessionBuilder, listener).await {
                panic!("{err}")
            }
        });

        Ok((Url::parse(&format!("http://127.0.0.1:{port}"))?, task))
    }
}
