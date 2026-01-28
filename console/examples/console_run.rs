use arrow::util::pretty::pretty_format_batches;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::{
    ConsoleControlServiceClient, DistributedExt, DistributedPhysicalOptimizerRule,
    RegisterWorkersRequest, WorkerResolver, display_plan_ascii,
};
use futures::TryStreamExt;
use std::error::Error;
use std::sync::Arc;
use structopt::StructOpt;
use url::Url;

#[derive(StructOpt)]
#[structopt(name = "console_run", about = "Run queries with console integration")]
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

    /// Console address
    #[structopt(long = "console-addr", default_value = "http://localhost:9090")]
    console_addr: String,
}

/// Register worker locations with console
async fn register_with_console(
    console_addr: &str,
    worker_urls: Vec<String>,
) -> Result<u32, Box<dyn Error>> {
    let mut client = ConsoleControlServiceClient::connect(console_addr.to_string()).await?;

    let request = RegisterWorkersRequest { worker_urls };

    let response = client.register_workers(request).await?;
    let response = response.into_inner();

    if !response.error_message.is_empty() {
        eprintln!("Registration warnings: {}", response.error_message);
    }

    Ok(response.workers_registered)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let localhost_resolver = LocalhostWorkerResolver {
        ports: args.cluster_ports.clone(),
    };

    // Register workers with console before query execution
    let worker_urls: Vec<String> = args
        .cluster_ports
        .iter()
        .map(|p| format!("http://localhost:{p}"))
        .collect();

    match register_with_console(&args.console_addr, worker_urls).await {
        Ok(count) => println!("Registered {count} workers with console"),
        Err(e) => {
            eprintln!("Warning: Failed to register with console: {e}");
            eprintln!("Continuing without console monitoring...");
        }
    }

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
