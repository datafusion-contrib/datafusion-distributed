use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    DistributedExt, DistributedMetricsFormat, DistributedPhysicalOptimizerRule, WorkerResolver,
    display_plan_ascii,
};
use futures::TryStreamExt;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use url::Url;

#[derive(StructOpt)]
#[structopt(name = "tpcds_runner", about = "Run TPC-DS with observability")]
struct Args {
    /// Comma-delimited list of worker ports (e.g. 8080,8081)
    #[structopt(long, use_delimiter = true)]
    cluster_ports: Vec<u16>,

    #[structopt(long, default_value = "1.0")]
    scale_factor: f64,

    #[structopt(long, default_value = "4")]
    parquet_partitions: usize,

    /// Specific query to run (e.g., "q1"), or "all" to run all queries
    #[structopt(long, default_value = "all")]
    query: String,

    /// Maximum number of concurrent queries. Defaults to all queries at once.
    /// Use --concurrency 1 for sequential execution.
    #[structopt(long)]
    concurrency: Option<usize>,

    /// Run the query and display the plan with execution metrics (EXPLAIN ANALYZE)
    #[structopt(long)]
    explain_analyze: bool,

    /// Whether the distributed plan should be rendered instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    run_queries(
        args.cluster_ports,
        args.scale_factor,
        args.parquet_partitions,
        &args.query,
        args.concurrency,
        args.explain_analyze,
        args.show_distributed_plan,
    )
    .await
}

async fn run_queries(
    cluster_ports: Vec<u16>,
    scale_factor: f64,
    parquet_partitions: usize,
    query_id: &str,
    concurrency: Option<usize>,
    explain_analyze: bool,
    show_distributed_plan: bool,
) -> Result<(), Box<dyn Error>> {
    use datafusion_distributed::test_utils::{benchmarks_common, tpcds};
    use std::fs;

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
        (1..=99).map(|i| format!("q{i}")).collect()
    } else {
        vec![query_id.to_string()]
    };

    let max_concurrent = concurrency.unwrap_or(queries.len());

    println!(
        "\nRunning {} queries (concurrency={})...\n",
        queries.len(),
        max_concurrent
    );

    // Resolve query SQL upfront
    let query_sqls: Vec<(String, String)> = queries
        .into_iter()
        .filter_map(|query| match tpcds::get_query(&query) {
            Ok(sql) => Some((query, sql)),
            Err(e) => {
                println!("Skipping {query}: {e}");
                None
            }
        })
        .collect();

    let semaphore = Arc::new(Semaphore::new(max_concurrent));
    let mut tasks = JoinSet::new();

    for (query_id, query_sql) in query_sqls {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let ctx = ctx.clone();

        tasks.spawn(async move {
            use std::time::Instant;

            println!("Running {query_id}");
            let start = Instant::now();

            let result =
                run_single_query(&ctx, &query_sql, explain_analyze, show_distributed_plan).await;
            drop(permit);

            match result {
                Ok(batches) => {
                    let duration = start.elapsed();
                    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
                    println!("{query_id} completed in {duration:?} ({row_count} rows)");
                }
                Err(e) => {
                    println!("{query_id} failed: {e}");
                }
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(e) = result {
            eprintln!("Task panicked: {e}");
        }
    }

    println!("\nAll queries finished.");

    Ok(())
}

async fn run_single_query(
    ctx: &SessionContext,
    query_sql: &str,
    explain_analyze: bool,
    show_distributed_plan: bool,
) -> Result<Vec<datafusion::arrow::array::RecordBatch>, Box<dyn Error + Send + Sync>> {
    let df = ctx.sql(query_sql).await?;
    let plan = df.create_physical_plan().await?;
    if show_distributed_plan {
        println!("{}", display_plan_ascii(plan.as_ref(), false));
        return Ok(vec![]);
    }
    let stream = execute_stream(plan.clone(), ctx.task_ctx())?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    if explain_analyze {
        let output =
            datafusion_distributed::explain_analyze(plan, DistributedMetricsFormat::Aggregated)?;
        println!("{output}");
    }
    // let formatted = pretty_format_batches(&batches)?;
    // println!("{formatted}");
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
