use arrow::util::pretty::pretty_format_batches;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use dashmap::{DashMap, Entry};
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::{
    BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedPhysicalOptimizerRule,
};
use futures::TryStreamExt;
use std::error::Error;
use std::sync::Arc;
use structopt::StructOpt;
use tonic::transport::Channel;
use url::Url;

#[derive(StructOpt)]
#[structopt(name = "run", about = "A localhost Distributed DataFusion runner")]
struct Args {
    #[structopt()]
    query: String,

    // --cluster-ports 8080,8081,8082
    #[structopt(long = "cluster-ports", use_delimiter = true)]
    cluster_ports: Vec<u16>,

    #[structopt(long)]
    explain: bool,

    #[structopt(long, default_value = "3")]
    network_shuffle_tasks: usize,

    #[structopt(long, default_value = "3")]
    network_coalesce_tasks: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let localhost_resolver = LocalhostChannelResolver {
        ports: args.cluster_ports,
        cached: DashMap::new(),
    };

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_channel_resolver(localhost_resolver)
        .with_physical_optimizer_rule(Arc::new(
            DistributedPhysicalOptimizerRule::new()
                .with_network_coalesce_tasks(args.network_coalesce_tasks)
                .with_network_shuffle_tasks(args.network_shuffle_tasks),
        ))
        .build();

    let ctx = SessionContext::from(state);

    ctx.register_parquet(
        "flights_1m",
        "testdata/flights-1m.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    ctx.register_parquet("weather", "testdata/weather", ParquetReadOptions::default())
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
