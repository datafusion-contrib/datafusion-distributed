use crate::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedSessionBuilderContext,
};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use hyper_util::rt::TokioIo;
use tonic::transport::{Endpoint, Server};

const DUMMY_URL: &str = "http://localhost:50051";

/// [ChannelResolver] implementation that returns gRPC clients backed by an in-memory
/// tokio duplex rather than a TCP connection.
#[derive(Clone)]
pub struct InMemoryChannelResolver {
    channel: FlightServiceClient<BoxCloneSyncChannel>,
}

impl Default for InMemoryChannelResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryChannelResolver {
    pub fn new() -> Self {
        let (client, server) = tokio::io::duplex(1024 * 1024);

        let mut client = Some(client);
        let channel = Endpoint::try_from(DUMMY_URL)
            .expect("Invalid dummy URL for building an endpoint. This should never happen")
            .connect_with_connector_lazy(tower::service_fn(move |_| {
                let client = client
                    .take()
                    .expect("Client taken twice. This should never happen");
                async move { Ok::<_, std::io::Error>(TokioIo::new(client)) }
            }));

        let this = Self {
            channel: FlightServiceClient::new(BoxCloneSyncChannel::new(channel)),
        };
        let this_clone = this.clone();

        let endpoint =
            ArrowFlightEndpoint::try_new(move |ctx: DistributedSessionBuilderContext| {
                let this = this.clone();
                async move {
                    let builder = SessionStateBuilder::new()
                        .with_default_features()
                        .with_distributed_channel_resolver(this)
                        .with_runtime_env(ctx.runtime_env.clone());
                    Ok(builder.build())
                }
            })
            .unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(endpoint))
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });

        this_clone
    }
}

#[async_trait]
impl ChannelResolver for InMemoryChannelResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, DataFusionError> {
        Ok(vec![url::Url::parse(DUMMY_URL).unwrap()])
    }

    async fn get_flight_client_for_url(
        &self,
        _: &url::Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        Ok(self.channel.clone())
    }
}
