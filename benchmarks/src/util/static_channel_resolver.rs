use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion_distributed::{BoxCloneSyncChannel, ChannelResolver, create_flight_client};
use std::collections::HashMap;
use tonic::transport::Channel;
use url::Url;

#[derive(Clone)]
pub struct StaticChannelResolver {
    channels: HashMap<Url, BoxCloneSyncChannel>,
}

impl StaticChannelResolver {
    pub fn new<N: TryInto<String>, I: IntoIterator<Item = N>>(urls: I) -> Self
    where
        N::Error: std::fmt::Debug,
    {
        let mut channels = HashMap::new();
        for url in urls.into_iter() {
            let url = url.try_into().unwrap();
            let url = Url::parse(&url).unwrap();
            let endpoint = Channel::from_shared(url.to_string()).unwrap();
            let channel = endpoint.connect_lazy();
            channels.insert(url, BoxCloneSyncChannel::new(channel));
        }
        Self { channels }
    }
}

#[async_trait]
impl ChannelResolver for StaticChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.channels.keys().cloned().collect())
    }

    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        let channel = self.channels.get(url).ok_or_else(|| {
            exec_datafusion_err!(
                "Url {url} not found, available urls are: {:?}",
                self.get_urls().unwrap()
            )
        })?;
        Ok(create_flight_client(channel.clone()))
    }
}
