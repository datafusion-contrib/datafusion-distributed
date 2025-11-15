use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion_distributed::{BoxCloneSyncChannel, ChannelResolver};
use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use std::sync::Arc;
use tonic::transport::Channel;
use url::Url;

const MAX_DECODING_MSG_SIZE: usize = 2 * 1024 * 1024 * 1024;

/// [ChannelResolver] implementation that uses grpc for resolving nodes hosted at the
/// provided ip addresses.
#[derive(Clone)]
pub struct DistributedChannelResolver {
    addresses: Vec<Url>,
    channels: Arc<moka::sync::Cache<url::Url, ChannelCacheValue>>,
}

type ChannelCacheValue = Shared<
    BoxFuture<'static, Result<FlightServiceClient<BoxCloneSyncChannel>, Arc<DataFusionError>>>,
>;

impl DistributedChannelResolver {
    /// Builds a [DistributedChannelResolver] from a list of endpoint URLs or IP:port pairs.
    pub async fn try_new(endpoints: Vec<String>) -> Result<Self, DataFusionError> {
        let mut addresses = Vec::new();

        for endpoint in endpoints {
            // Add http:// prefix if missing
            let url_str = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
                endpoint.clone()
            } else {
                format!("http://{}", endpoint)
            };

            let url = Url::parse(&url_str).map_err(|e| {
                internal_datafusion_err!("Error parsing endpoint URL '{}': {}", endpoint, e)
            })?;
            addresses.push(url);
        }

        Ok(Self {
            addresses,
            channels: Arc::new(moka::sync::Cache::new(1000)),
        })
    }
}

#[async_trait]
impl ChannelResolver for DistributedChannelResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, DataFusionError> {
        Ok(self.addresses.clone())
    }

    async fn get_flight_client_for_url(
        &self,
        url: &url::Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        let url_clone = url.to_string();
        let result = self
            .channels
            .get_with(url.clone(), move || {
                async move {
                    let unconnected_channel = Channel::from_shared(url_clone)
                        .map_err(|e| internal_datafusion_err!("Invalid URI: {e}"))?;
                    let channel = unconnected_channel.connect().await.map_err(|e| {
                        DataFusionError::Execution(format!("Error connecting to url: {e}"))
                    })?;

                    // Apply layers to the channel.
                    let channel = tower::ServiceBuilder::new().service(channel);

                    let client = FlightServiceClient::new(BoxCloneSyncChannel::new(channel))
                        .max_decoding_message_size(MAX_DECODING_MSG_SIZE);

                    Ok(client)
                }
                .boxed()
                .shared()
            })
            .await;

        match result {
            Ok(result) => Ok(result),
            Err(err) => {
                self.channels.remove(url);
                Err(DataFusionError::Shared(err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_endpoint_parsing() {
        let endpoints = vec![
            "127.0.0.1:8080".to_string(),
            "192.168.1.1:9090".to_string(),
            "http://example.com:8081".to_string(),
        ];

        let resolver = DistributedChannelResolver::try_new(endpoints)
            .await
            .unwrap();
        let urls = resolver.get_urls().unwrap();

        assert_eq!(urls.len(), 3);
        assert_eq!(urls[0].as_str(), "http://127.0.0.1:8080/");
        assert_eq!(urls[1].as_str(), "http://192.168.1.1:9090/");
        assert_eq!(urls[2].as_str(), "http://example.com:8081/");
    }

    #[tokio::test]
    async fn test_ipv6_endpoint_parsing() {
        let endpoints = vec![
            "[::1]:8080".to_string(),
            "https://[2001:db8::1]:9090".to_string(),
        ];

        let resolver = DistributedChannelResolver::try_new(endpoints)
            .await
            .unwrap();
        let urls = resolver.get_urls().unwrap();

        assert_eq!(urls.len(), 2);
        assert_eq!(urls[0].as_str(), "http://[::1]:8080/");
        assert_eq!(urls[1].as_str(), "https://[2001:db8::1]:9090/");
    }
}
