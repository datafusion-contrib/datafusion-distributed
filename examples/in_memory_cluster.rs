use arrow::util::pretty::pretty_format_batches;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::{
    BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedPhysicalOptimizerRule, Worker,
    WorkerQueryContext, WorkerResolver, create_flight_client, display_plan_ascii,
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
    about = "Run a query in an in-memory Distributed DataFusion cluster"
)]
struct Args {
    /// The SQL query to run.
    #[structopt()]
    query: String,

    /// Whether the distributed plan should be rendered instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_worker_resolver(InMemoryWorkerResolver)
        .with_distributed_channel_resolver(InMemoryChannelResolver::new())
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        .with_distributed_files_per_task(1)?
        .build();

    let ctx = SessionContext::from(state);

    ctx.register_parquet("weather", "testdata/weather", ParquetReadOptions::default())
        .await?;

    let df = ctx.sql(&args.query).await?;
    if args.show_distributed_plan {
        let plan = df.create_physical_plan().await?;
        println!("{}", display_plan_ascii(plan.as_ref(), false));
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
            channel: create_flight_client(BoxCloneSyncChannel::new(channel)),
        };
        let this_clone = this.clone();

        let endpoint = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
            let this = this.clone();
            async move { Ok(ctx.builder.with_distributed_channel_resolver(this).build()) }
        });

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
    async fn get_flight_client_for_url(
        &self,
        _: &url::Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        Ok(self.channel.clone())
    }
}

struct InMemoryWorkerResolver;

impl WorkerResolver for InMemoryWorkerResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, DataFusionError> {
        Ok(vec![url::Url::parse(DUMMY_URL).unwrap(); 16]) // simulate 16 workers.
    }
}
