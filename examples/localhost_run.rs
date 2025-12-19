use arrow::util::pretty::pretty_format_batches;
use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use dashmap::{DashMap, Entry};
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::{
    BoxCloneSyncChannel, ChannelResolver, DistributedExt, DistributedPhysicalOptimizerRule,
    display_plan_ascii,
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
    /// The SQL query to run.
    #[structopt()]
    query: String,

    /// The ports holding Distributed DataFusion workers.
    #[structopt(long = "cluster-ports", use_delimiter = true)]
    cluster_ports: Vec<u16>,

    /// Whether the distributed plan should be rendered instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
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
