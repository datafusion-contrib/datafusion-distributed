use crate::{
    BoxCloneSyncChannel, ChannelResolver, DefaultSessionBuilder, DistributedExt,
    MappedWorkerSessionBuilderExt, Worker, WorkerResolver, WorkerSessionBuilder,
    create_flight_client,
};
use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use hyper_util::rt::TokioIo;
use tonic::transport::{Endpoint, Server};

const DUMMY_URL: &str = "http://localhost:50051";

/// [ChannelResolver] implementation that returns gRPC clients backed by an in-memory
/// tokio duplex rather than a TCP connection.
#[derive(Clone)]
pub struct InMemoryChannelResolver {
    channel: FlightServiceClient<BoxCloneSyncChannel>,
}

impl InMemoryChannelResolver {
    /// Build an [InMemoryChannelResolver] with a custom [WorkerSessionBuilder].
    /// This allows you to inject your own DataFusion extensions in the in-memory worker
    /// spawned by this method.
    pub fn from_session_builder(
        builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    ) -> Self {
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
        };
        let this_clone = this.clone();

        let endpoint = Worker::from_session_builder(builder.map(move |builder| {
            let this = this.clone();
            Ok(builder.with_distributed_channel_resolver(this).build())
        }));

        tokio::spawn(async move {
            Server::builder()
                .add_service(endpoint.into_flight_server())
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });

        this_clone
    }
}

impl Default for InMemoryChannelResolver {
    fn default() -> Self {
        Self::from_session_builder(DefaultSessionBuilder)
    }
}

#[async_trait]
impl ChannelResolver for InMemoryChannelResolver {
    async fn get_flight_client_for_url(
        &self,
        _: &url::Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        Ok(self.channel.clone())
    }
}

pub struct InMemoryWorkerResolver {
    n_workers: usize,
}

impl InMemoryWorkerResolver {
    pub fn new(n_workers: usize) -> Self {
        Self { n_workers }
    }
}

impl WorkerResolver for InMemoryWorkerResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, DataFusionError> {
        // Set to a high number so that the distributed planner does not limit the maximum
        // spawned tasks to just 1.
        Ok(vec![url::Url::parse(DUMMY_URL).unwrap(); self.n_workers])
    }
}
