use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use tonic::body::BoxBody;
use url::Url;

pub(crate) fn set_channel_resolver(
    cfg: &mut SessionConfig,
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
) {
    cfg.set_extension(Arc::new(ChannelResolverExtension(Arc::new(
        channel_resolver,
    ))));
}

pub(crate) fn get_channel_resolver(
    cfg: &SessionConfig,
) -> Option<Arc<dyn ChannelResolver + Send + Sync>> {
    cfg.get_extension::<ChannelResolverExtension>()
        .map(|cm| cm.0.clone())
}

#[derive(Clone)]
struct ChannelResolverExtension(Arc<dyn ChannelResolver + Send + Sync>);

pub type BoxCloneSyncChannel = tower::util::BoxCloneSyncService<
    http::Request<BoxBody>,
    http::Response<BoxBody>,
    tonic::transport::Error,
>;

/// Abstracts networking details so that users can implement their own network resolution
/// mechanism.
#[async_trait]
pub trait ChannelResolver {
    /// Gets all available worker URLs. Used during stage assignment.
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
    /// For a given URL, get a channel for communicating to it.
    async fn get_channel_for_url(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError>;
}

#[async_trait]
impl ChannelResolver for Arc<dyn ChannelResolver + Send + Sync> {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.as_ref().get_urls()
    }

    async fn get_channel_for_url(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError> {
        self.as_ref().get_channel_for_url(url).await
    }
}
