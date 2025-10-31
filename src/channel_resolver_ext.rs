use crate::DistributedConfig;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use tonic::body::Body;
use url::Url;

pub(crate) fn set_distributed_channel_resolver(
    cfg: &mut SessionConfig,
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    let channel_resolver_ext = ChannelResolverExtension(Arc::new(channel_resolver));
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_channel_resolver = channel_resolver_ext;
    } else {
        opts.extensions.insert(DistributedConfig {
            __private_channel_resolver: channel_resolver_ext,
            ..Default::default()
        });
    }
}

pub(crate) fn get_distributed_channel_resolver(
    cfg: &SessionConfig,
) -> Result<Arc<dyn ChannelResolver + Send + Sync>, DataFusionError> {
    let opts = cfg.options();
    let Some(distributed_cfg) = opts.extensions.get::<DistributedConfig>() else {
        return exec_err!("ChannelResolver not present in the session config");
    };
    Ok(Arc::clone(&distributed_cfg.__private_channel_resolver.0))
}

#[derive(Clone)]
pub(crate) struct ChannelResolverExtension(pub(crate) Arc<dyn ChannelResolver + Send + Sync>);

pub type BoxCloneSyncChannel = tower::util::BoxCloneSyncService<
    http::Request<Body>,
    http::Response<Body>,
    tonic::transport::Error,
>;

/// Abstracts networking details so that users can implement their own network resolution
/// mechanism.
///
/// # Implementation Note
///
/// When implementing `get_flight_client_for_url`, it is recommended to use the
/// [`create_flight_client`] helper function to ensure clients are configured with
/// appropriate message size limits for internal communication. This helps avoid message
/// size errors when transferring large datasets.
#[async_trait]
pub trait ChannelResolver {
    /// Gets all available worker URLs. Used during stage assignment.
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
    /// For a given URL, get an Arrow Flight client for communicating to it.
    ///
    /// Consider using [`create_flight_client`] to create the client with appropriate
    /// default message size limits.
    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError>;
}

#[async_trait]
impl ChannelResolver for Arc<dyn ChannelResolver + Send + Sync> {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.as_ref().get_urls()
    }

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
