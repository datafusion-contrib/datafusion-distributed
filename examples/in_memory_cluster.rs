use arrow::util::pretty::pretty_format_batches;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedPhysicalOptimizerRule, DistributedSessionBuilderContext,
};
use futures::TryStreamExt;
use hyper_util::rt::TokioIo;
use std::error::Error;
use std::sync::Arc;
use structopt::StructOpt;
use tonic::transport::{Endpoint, Server};

#[derive(StructOpt)]
#[structopt(
    name = "run",
    about = "An in-memory cluster Distributed DataFusion runner"
)]
struct Args {
    #[structopt()]
    query: String,

    #[structopt(long)]
    explain: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_channel_resolver(InMemoryChannelResolver::new())
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule::new()))
        .build();

    let ctx = SessionContext::from(state);

    ctx.register_parquet(
        "flights_1m",
        "testdata/flights-1m.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    ctx.register_parquet(
        "weather",
        "testdata/weather.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    let df = ctx.sql(&args.query).await?;
    if args.explain {
        let plan = df.create_physical_plan().await?;
        let display = displayable(plan.as_ref()).indent(true).to_string();
        println!("{display}");
    } else {
        let stream = df.execute_stream().await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        let formatted = pretty_format_batches(&batches)?;
        println!("{formatted}");
    }
    Ok(())
}

const DUMMY_URL: &str = "http://localhost:50051";

/// [ChannelResolver] implementation that returns gRPC clients baked by an in-memory
/// tokio duplex rather than a TCP connection.
#[derive(Clone)]
struct InMemoryChannelResolver {
    channel: FlightServiceClient<BoxCloneSyncChannel>,
}

impl InMemoryChannelResolver {
    fn new() -> Self {
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
