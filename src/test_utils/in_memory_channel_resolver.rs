use crate::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedSessionBuilderContext, create_flight_client,
};
use arrow_flight::flight_service_client::FlightServiceClient;
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
    n_workers: usize,
}

impl InMemoryChannelResolver {
    pub fn new(n_workers: usize) -> Self {
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
            channel: create_flight_client(BoxCloneSyncChannel::new(channel)),
            n_workers,
        };
        let this_clone = this.clone();

        let endpoint =
            ArrowFlightEndpoint::try_new(move |ctx: DistributedSessionBuilderContext| {
                let this = this.clone();
                async move {
                    let builder = SessionStateBuilder::new()
                        .with_default_features()
                        .with_distributed_execution(this)
                        .with_runtime_env(ctx.runtime_env.clone());
                    Ok(builder.build())
                }
            })
            .unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(endpoint.into_flight_server())
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });

        this_clone
    }
}

#[async_trait]
impl ChannelResolver for InMemoryChannelResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, DataFusionError> {
        // Set to a high number so that the distributed planner does not limit the maximum
        // spawned tasks to just 1.
        Ok(vec![url::Url::parse(DUMMY_URL).unwrap(); self.n_workers])
    }

    async fn get_flight_client_for_url(
        &self,
        _: &url::Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        Ok(self.channel.clone())
    }
}
