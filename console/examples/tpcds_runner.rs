use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    DistributedExt, DistributedPhysicalOptimizerRule, WorkerResolver,
};
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use structopt::StructOpt;
use url::Url;

#[derive(StructOpt)]
#[structopt(name = "tpcds_runner", about = "Run TPC-DS with observability")]
struct Args {
    /// Start a worker with observability service
    /// Run TPC-DS queries against workers
    #[structopt(long, use_delimiter = true)]
    cluster_ports: Vec<u16>,

    #[structopt(long, default_value = "1.0")]
    scale_factor: f64,

    #[structopt(long, default_value = "4")]
    parquet_partitions: usize,

    /// Specific query to run (e.g., "q1"), or "all" to run all queries
    #[structopt(long, default_value = "all")]
    query: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    run_queries(
        args.cluster_ports,
        args.scale_factor,
        args.parquet_partitions,
        &args.query,
    )
    .await
}

async fn run_queries(
    cluster_ports: Vec<u16>,
    scale_factor: f64,
    parquet_partitions: usize,
    query_id: &str,
) -> Result<(), Box<dyn Error>> {
    use datafusion_distributed::test_utils::{benchmarks_common, tpcds};
    use std::fs;
    use std::time::Instant;
    use tokio::time::sleep;

    println!(
        "Running TPC-DS queries (SF={scale_factor}, partitions={parquet_partitions}) against workers: {cluster_ports:?}"
    );

    // Generate test data if needed
    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "testdata/tpcds/correctness_sf{scale_factor}_partitions{parquet_partitions}",
    ));

    if !fs::exists(&data_dir).unwrap_or(false) {
        println!("Generating TPC-DS data at {data_dir:?}...");
        tpcds::generate_data(&data_dir, scale_factor, parquet_partitions).await?;
    }

    // Create distributed context
    let localhost_resolver = LocalhostWorkerResolver {
        ports: cluster_ports,
    };

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_worker_resolver(localhost_resolver)
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        .build();

    let ctx = SessionContext::from(state);
    let ctx = ctx
        .with_distributed_files_per_task(2)?
        .with_distributed_cardinality_effect_task_scale_factor(2.0)?
        .with_distributed_broadcast_joins(true)?;

    benchmarks_common::register_tables(&ctx, &data_dir).await?;

    // Determine which queries to run
    let queries: Vec<String> = if query_id == "all" {
        // Run all TPC-DS queries (q1 through q99)
        (1..=99).map(|i| format!("q{i}")).collect()
    } else {
        vec![query_id.to_string()]
    };

    println!("\nRunning {} queries...\n", queries.len());

    for query in queries {
        let query_sql = match tpcds::get_query(&query) {
            Ok(sql) => sql,
            Err(e) => {
                println!("Skipping {query}: {e}\n");
                continue;
            }
        };

        // Add sleep to observe "completed" state in console
        sleep(tokio::time::Duration::from_millis(1000)).await;

        println!("Running {query}");

        let start = Instant::now();

        match run_single_query(&ctx, &query_sql).await {
            Ok(batches) => {
                let duration = start.elapsed();
                let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

                println!("{query} completed in {duration:?}");
                println!("\tRows returned: {row_count}");
            }
            Err(e) => {
                println!("{query} failed: {e}");
            }
        }

        println!();
    }

    Ok(())
}

async fn run_single_query(
    ctx: &SessionContext,
    query_sql: &str,
) -> Result<Vec<datafusion::arrow::array::RecordBatch>, Box<dyn Error>> {
    use futures::StreamExt;

    let df = ctx.sql(query_sql).await?;
    let stream = df.execute_stream().await?;
    let batches = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}

#[derive(Clone)]
struct LocalhostWorkerResolver {
    ports: Vec<u16>,
}

#[async_trait]
impl WorkerResolver for LocalhostWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.ports
            .iter()
            .map(|port| {
                let url_string = format!("http://localhost:{port}");
                Url::parse(&url_string).map_err(|e| DataFusionError::External(Box::new(e)))
            })
            .collect::<Result<Vec<Url>, _>>()
    }
}
