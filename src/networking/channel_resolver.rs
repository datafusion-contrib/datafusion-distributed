use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::prelude::SessionConfig;
use futures::FutureExt;
use futures::future::Shared;
use std::io;
use std::sync::{Arc, LazyLock};
use tonic::body::Body;
use tonic::codegen::BoxFuture;
use tonic::transport::Channel;
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
    /// Consider using [`create_flight_client`] to create the client with appropriate
    /// default message size limits.
    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        let channel = CHANNEL_CACHE.get_with_by_ref(url, move || {
            let url = url.clone();
            async move {
                let endpoint = Channel::from_shared(url.to_string())
                    .map_err(|err| DataFusionError::IoError(io::Error::other(err.to_string())))?;
                let channel = endpoint
                    .connect()
                    .await
                    .map_err(|err| DataFusionError::IoError(io::Error::other(err.to_string())))?;
                Ok(create_flight_client(BoxCloneSyncChannel::new(channel)))
            }
            .boxed()
            .shared()
        });

        channel.await.map_err(|err| {
            CHANNEL_CACHE.invalidate(url);
            DataFusionError::Shared(err)
        })
    }
}

pub(crate) fn set_distributed_channel_resolver(
    cfg: &mut SessionConfig,
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    let channel_resolver = ChannelResolverExtension::new(channel_resolver);
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_channel_resolver = channel_resolver;
    } else {
        set_distributed_option_extension(cfg, DistributedConfig {
            __private_channel_resolver: channel_resolver,
            ..Default::default()
        }).expect("Calling set_distributed_option_extension with a default DistributedConfig should never fail");
    }
}

pub(crate) fn get_distributed_channel_resolver(
    cfg: &SessionConfig,
) -> Result<ChannelResolverExtension, DataFusionError> {
    let opts = cfg.options();
    let Some(distributed_cfg) = opts.extensions.get::<DistributedConfig>() else {
        return Ok(ChannelResolverExtension::default());
    };
    Ok(distributed_cfg.__private_channel_resolver.clone())
}

pub type BoxCloneSyncChannel = tower::util::BoxCloneSyncService<
    http::Request<Body>,
    http::Response<Body>,
    tonic::transport::Error,
>;

type ChannelCacheValue =
    Shared<BoxFuture<FlightServiceClient<BoxCloneSyncChannel>, Arc<DataFusionError>>>;
// TODO: be smarter here, use something like a RLU cache for example
static CHANNEL_CACHE: LazyLock<moka::sync::Cache<Url, ChannelCacheValue>> =
    LazyLock::new(|| moka::sync::Cache::new(1000));

#[derive(Clone)]
pub(crate) struct ChannelResolverExtension {
    inner: Arc<dyn ChannelResolver + Send + Sync>,
}

impl Default for ChannelResolverExtension {
    fn default() -> Self {
        struct DefaultChannelResolver;
        impl ChannelResolver for DefaultChannelResolver {}
        Self::new(DefaultChannelResolver)
    }
}

impl ChannelResolverExtension {
    pub(crate) fn new(inner: impl ChannelResolver + Send + Sync + 'static) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        self.inner.get_flight_client_for_url(url).await
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
