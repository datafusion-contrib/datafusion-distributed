use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::{
    DefaultChannelResolver, DistributedExt, DistributedPhysicalOptimizerRule, GetWorkerInfoRequest,
    WorkerResolver, create_worker_client, display_plan_ascii,
};
use futures::TryStreamExt;
use std::error::Error;
use std::sync::Arc;
use structopt::StructOpt;
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

    /// Only use workers reporting this version (via GetWorkerInfo).
    /// When omitted, all workers in --cluster-ports are used.
    #[structopt(long)]
    version: Option<String>,

    /// Whether the distributed plan should be rendered instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
}

/// Returns `true` if the worker at `url` reports `expected_version` via
/// `GetWorkerInfo`. Returns `false` if the worker is unreachable, returns
/// an error, or reports a different version.
async fn worker_has_version(
    channel_resolver: &DefaultChannelResolver,
    url: &Url,
    expected_version: &str,
) -> bool {
    let Ok(channel) = channel_resolver.get_channel(url).await else {
        return false;
    };
    let mut client = create_worker_client(channel);
    let Ok(response) = client.get_worker_info(GetWorkerInfoRequest {}).await else {
        return false;
    };
    response.into_inner().version_number == expected_version
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let ports = if let Some(target_version) = &args.version {
        // Filter workers by version before building the session.
        let channel_resolver = DefaultChannelResolver::default();
        let mut compatible = Vec::new();
        for &port in &args.cluster_ports {
            let url = Url::parse(&format!("http://localhost:{port}"))?;
            if worker_has_version(&channel_resolver, &url, target_version).await {
                compatible.push(port);
            } else {
                println!("Excluding worker on port {port} (version mismatch)");
            }
        }
        if compatible.is_empty() {
            return Err(format!("No workers matched version '{target_version}'").into());
        }
        println!(
            "Using {}/{} workers matching version '{target_version}'\n",
            compatible.len(),
            args.cluster_ports.len(),
        );
        compatible
    } else {
        args.cluster_ports
    };

    let localhost_resolver = LocalhostWorkerResolver { ports };

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_worker_resolver(localhost_resolver)
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
struct LocalhostWorkerResolver {
    ports: Vec<u16>,
}

#[async_trait]
impl WorkerResolver for LocalhostWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }
}
