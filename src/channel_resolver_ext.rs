use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;
use tonic::body::BoxBody;
use url::Url;

pub(crate) fn set_distributed_channel_resolver(
    cfg: &mut SessionConfig,
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
) {
    cfg.set_extension(Arc::new(ChannelResolverExtension(Arc::new(
        channel_resolver,
    ))));
}

pub(crate) fn get_distributed_channel_resolver(
    cfg: &SessionConfig,
) -> Result<Arc<dyn ChannelResolver + Send + Sync>, DataFusionError> {
    cfg.get_extension::<ChannelResolverExtension>()
        .map(|cm| cm.0.clone())
        .ok_or_else(|| exec_datafusion_err!("ChannelResolver not present in the session config"))
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
    /// For a given URL, get an Arrow Flight client for communicating to it.
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
