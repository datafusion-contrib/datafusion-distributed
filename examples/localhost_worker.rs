use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use dashmap::{DashMap, Entry};
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedSessionBuilderContext,
};
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;
use tonic::transport::{Channel, Server};
use url::Url;

#[derive(StructOpt)]
#[structopt(name = "localhost_worker", about = "A localhost DataFusion worker")]
struct Args {
    #[structopt(default_value = "8080")]
    port: u16,

    // --cluster-ports 8080,8081,8082
    #[structopt(long = "cluster-ports", use_delimiter = true)]
    cluster_ports: Vec<u16>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let localhost_resolver = LocalhostChannelResolver {
        ports: args.cluster_ports,
        cached: DashMap::new(),
    };

    let endpoint = ArrowFlightEndpoint::try_new(move |ctx: DistributedSessionBuilderContext| {
        let local_host_resolver = localhost_resolver.clone();
        async move {
            Ok(SessionStateBuilder::new()
                .with_runtime_env(ctx.runtime_env)
                .with_distributed_channel_resolver(local_host_resolver)
                .with_default_features()
                .build())
        }
    })?;

    Server::builder()
        .add_service(FlightServiceServer::new(endpoint))
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port))
        .await?;

    Ok(())
}

#[derive(Clone)]
struct LocalhostChannelResolver {
    ports: Vec<u16>,
    cached: DashMap<Url, FlightServiceClient<BoxCloneSyncChannel>>,
}

#[async_trait]
impl ChannelResolver for LocalhostChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }

    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        match self.cached.entry(url.clone()) {
            Entry::Occupied(v) => Ok(v.get().clone()),
            Entry::Vacant(v) => {
                let channel = Channel::from_shared(url.to_string())
                    .unwrap()
                    .connect_lazy();
                let channel = FlightServiceClient::new(BoxCloneSyncChannel::new(channel));
                v.insert(channel.clone());
                Ok(channel)
            }
        }
    }
}
