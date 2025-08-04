use async_trait::async_trait;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::prelude::{SessionConfig, SessionContext};
use delegate::delegate;
use std::sync::Arc;
use tonic::body::BoxBody;
use url::Url;

#[derive(Clone)]
pub struct ChannelManager(Arc<dyn ChannelResolver + Send + Sync>);

impl ChannelManager {
    pub fn new(resolver: impl ChannelResolver + Send + Sync + 'static) -> Self {
        Self(Arc::new(resolver))
    }
}

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

impl ChannelManager {
    delegate! {
        to self.0 {
            pub fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
            pub async fn get_channel_for_url(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError>;
        }
    }
}

impl TryInto<ChannelManager> for &SessionConfig {
    type Error = DataFusionError;

    fn try_into(self) -> Result<ChannelManager, Self::Error> {
        Ok(self
            .get_extension::<ChannelManager>()
            .ok_or_else(|| internal_datafusion_err!("No extension ChannelManager"))?
            .as_ref()
            .clone())
    }
}

impl TryInto<ChannelManager> for &TaskContext {
    type Error = DataFusionError;

    fn try_into(self) -> Result<ChannelManager, Self::Error> {
        self.session_config().try_into()
    }
}

impl TryInto<ChannelManager> for &SessionContext {
    type Error = DataFusionError;

    fn try_into(self) -> Result<ChannelManager, Self::Error> {
        self.task_ctx().as_ref().try_into()
    }
}
