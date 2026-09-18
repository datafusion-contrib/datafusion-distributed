use async_trait::async_trait;
use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::DataFusionError;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use datafusion_distributed::test_utils::work_unit_file_scan::{
    WorkUnitFileScanCodec, WorkUnitFileScanConfig, work_unit_file_scan_desired_task_count,
    work_unit_file_scan_scale_up_leaf_node,
};
use datafusion_distributed::{
    ChannelResolver, DistributedExt, DistributedMetricsFormat, NetworkBoundaryExt,
    SessionStateBuilderExt, Stage, Worker, WorkerQueryContext, WorkerResolver, display_plan_ascii,
    get_distributed_channel_resolver, get_distributed_worker_resolver,
    rewrite_distributed_plan_with_metrics,
};
use futures::{StreamExt, TryFutureExt};
use log::{error, info, warn};
use object_store::aws::AmazonS3Builder;
use serde::Serialize;
use sketches_ddsketch::{Config, DDSketch};
use std::error::Error;
use std::fmt::Display;
use std::io;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use structopt::StructOpt;
use tonic::transport::Server;
use url::Url;

fn configure_session(builder: SessionStateBuilder) -> SessionStateBuilder {
    // Comment the code below in case this crate needs to stop depending on iceberg.
    use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};
    let builder = builder
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .with_iceberg_column_stats_enabled(true);

    builder
}

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Network and object-store options for the remote benchmark worker.
#[derive(Debug, StructOpt, Clone)]
#[structopt(about = "worker spawn command")]
struct RemoteWorkerOpt {
    /// The bucket name.
    #[structopt(long, default_value = "datafusion-distributed-benchmarks")]
    bucket: String,

    /// The headless worker service used for worker discovery.
    #[structopt(
        long,
        default_value = "datafusion-workers.benchmark-datafusion.svc.cluster.local"
    )]
    worker_dns_name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let cmd = RemoteWorkerOpt::from_args();

    const LISTENER_ADDR: &str = "0.0.0.0:9000";
    const WORKER_ADDR: &str = "0.0.0.0:9001";

    info!("Starting HTTP listener on {LISTENER_ADDR}...");
    let listener = tokio::net::TcpListener::bind(LISTENER_ADDR).await?;

    let self_url = get_self_url().await?;
    info!("Resolved self URL as {self_url}");

    // Register S3 object store
    let s3_url = Url::parse(&format!("s3://{}", cmd.bucket))?;

    info!("Building shared SessionContext for the whole lifetime of the HTTP listener...");
    let s3 = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(s3_url.host().unwrap().to_string())
            .build()?,
    );
    let runtime_env = Arc::new(RuntimeEnv::default());
    runtime_env.register_object_store(&s3_url, s3);

    let worker = Worker::from_session_builder(move |ctx: WorkerQueryContext| async move {
        let builder = ctx
            .builder
            .with_distributed_user_codec(WorkUnitFileScanCodec);
        Ok(configure_session(builder).build())
    })
    .with_runtime_env(Arc::clone(&runtime_env));

    let state_builder = SessionStateBuilder::new()
        .with_default_features()
        .with_runtime_env(runtime_env)
        .with_distributed_local_worker_context(worker.to_local_worker_context(self_url))
        .with_distributed_worker_resolver(DnsWorkerResolver::new(cmd.worker_dns_name.clone()))
        .with_distributed_planner()
        // Uncomment for enabling WorkUnitFileScans.
        // .with_physical_optimizer_rule(Arc::new(WorkUnitFileScanRule))
        .with_distributed_user_codec(WorkUnitFileScanCodec)
        .with_distributed_desired_task_count_handler(work_unit_file_scan_desired_task_count)
        .with_distributed_scale_up_leaf_node_handler(work_unit_file_scan_scale_up_leaf_node)
        .with_distributed_work_unit_feed(|dse: &DataSourceExec| {
            dse.data_source()
                .downcast_ref::<WorkUnitFileScanConfig>()
                .map(|v| &v.feed)
        });
    let state = configure_session(state_builder).build();
    let ctx = SessionContext::from(state);
    let ctx_clone = ctx.clone();

    let http_server = axum::serve(
        listener,
        Router::new()
            .route(
                "/info",
                get(move || async move {
                    let ctx = ctx_clone.clone();

                    let worker_resolver =
                        get_distributed_worker_resolver(ctx.state_ref().read().config())
                            .map_err(err)?;
                    let channel_resolver =
                        get_distributed_channel_resolver(ctx.task_ctx().as_ref());

                    let mut worker_urls = vec![];
                    let mut errors = vec![];
                    for worker_url in worker_resolver.get_urls().map_err(err)? {
                        if let Err(err) = channel_resolver
                            .get_worker_client_for_url(&worker_url)
                            .await
                        {
                            errors.push(err.to_string())
                        } else {
                            worker_urls.push(worker_url);
                        };
                    }
                    let worker_urls = worker_urls.into_iter().map(|v| v.to_string()).collect();

                    Ok::<_, (StatusCode, String)>(Json(WorkerInfo {
                        worker_urls,
                        git_commit_hash: built_info::GIT_COMMIT_HASH
                            .unwrap_or_default()
                            .to_string(),
                        build_time_utc: built_info::BUILT_TIME_UTC.to_string(),
                        errors,
                    }))
                }),
            )
            .route(
                "/",
                get(move |Query(params): Query<QueryParameters>| {
                    let ctx = ctx.clone();

                    async move {
                        let sql = params.get("sql").ok_or(err("Missing 'sql' parameter"))?;

                        let mut df_opt = None;
                        for sql in sql.split(";") {
                            if sql.trim().is_empty() {
                                continue;
                            }
                            let df = ctx.sql(sql).await.map_err(err)?;
                            df_opt = Some(df);
                        }
                        let Some(df) = df_opt else {
                            return Err(err("Empty 'sql' parameter"));
                        };

                        let start = Instant::now();

                        info!("Executing query...");
                        let abort_notifier = AbortNotifier::new("Query aborted");
                        let abort_notifier_clone = abort_notifier.clone();
                        let task = SpawnedTask::spawn(async move {
                            let _ = abort_notifier_clone;
                            loop {
                                tokio::time::sleep(Duration::from_secs(5)).await;
                                info!("Query still running...");
                            }
                        });
                        let physical = df.create_physical_plan().await.map_err(err)?;
                        let mut stream =
                            execute_stream(physical.clone(), ctx.task_ctx()).map_err(err)?;
                        let mut count = 0;
                        while let Some(batch) = stream.next().await {
                            count += batch.map_err(err)?.num_rows();
                            info!("Gathered {count} rows, query still in progress..")
                        }
                        let physical = rewrite_distributed_plan_with_metrics(
                            physical,
                            DistributedMetricsFormat::PerTask,
                        )
                        .await
                        .map_err(err)?;
                        let stats_q_error = stats_estimation_q_error(&physical);
                        let plan = display_plan_ascii(physical.as_ref(), true);
                        drop(task);

                        let mut task_count = 0;
                        physical
                            .apply(|plan| {
                                let Some(nb) = plan.as_network_boundary() else {
                                    return Ok(TreeNodeRecursion::Continue);
                                };
                                task_count += nb.input_stage().task_count();
                                Ok(TreeNodeRecursion::Continue)
                            })
                            .expect(".apply failed");

                        let elapsed = start.elapsed();
                        let ms = elapsed.as_secs_f64() * 1000.0;
                        info!("Finished executing query:\n{sql}\n\n{plan}");
                        info!("Returned {count} rows in {ms} ms");
                        abort_notifier.finished();

                        Ok::<_, (StatusCode, String)>(Json(QueryResult {
                            count,
                            plan,
                            elapsed_ms: ms,
                            tasks: task_count,
                            stats_q_error_p50: stats_q_error.map(|q_error| q_error.p50),
                            stats_q_error_p95: stats_q_error.map(|q_error| q_error.p95),
                        }))
                    }
                    .inspect_err(|(_, msg)| {
                        error!("Error executing query: {msg}");
                    })
                }),
            ),
    );
    let dns_worker_resolver = Arc::new(DnsWorkerResolver::new(cmd.worker_dns_name));
    let grpc_server = Server::builder()
        .add_service(worker.with_observability_service(dns_worker_resolver))
        .add_service(worker.into_worker_server())
        .serve(WORKER_ADDR.parse()?);

    info!("Started listener HTTP server in {LISTENER_ADDR}");
    info!("Started distributed DataFusion worker in {WORKER_ADDR}");

    tokio::select! {
        result = http_server => result?,
        result = grpc_server => result?,
    }

    Ok(())
}

#[allow(clippy::disallowed_types)]
type QueryParameters = std::collections::HashMap<String, String>;

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(Serialize)]
struct QueryResult {
    plan: String,
    count: usize,
    elapsed_ms: f64,
    tasks: usize,
    stats_q_error_p50: Option<f64>,
    stats_q_error_p95: Option<f64>,
}

#[derive(Serialize)]
struct WorkerInfo {
    worker_urls: Vec<String>,
    git_commit_hash: String,
    build_time_utc: String,
    errors: Vec<String>,
}

#[derive(Clone, Copy)]
struct StatsEstimationQError {
    p50: f64,
    p95: f64,
}

fn stats_estimation_q_error(plan: &Arc<dyn ExecutionPlan>) -> Option<StatsEstimationQError> {
    let mut boundary_q_errors = DDSketch::new(Config::defaults());

    let _ = plan.apply(|node| {
        if let Some(boundary) = node.as_network_boundary()
            && let Stage::Local(input_stage) = boundary.input_stage()
            && let Some(sampled_bytes) = metric_total(&input_stage.metrics_set, "sampled_bytes")
            && let Some(actual_bytes) = node
                .metrics()
                .and_then(|metrics| metric_total(&metrics, "output_bytes"))
        {
            boundary_q_errors.add(q_error(sampled_bytes, actual_bytes));
        }
        Ok(TreeNodeRecursion::Continue)
    });

    Some(StatsEstimationQError {
        p50: boundary_q_errors.quantile(0.50).ok().flatten()?,
        p95: boundary_q_errors.quantile(0.95).ok().flatten()?,
    })
}

fn metric_total(metrics: &MetricsSet, name: &str) -> Option<usize> {
    metrics
        .sum(|metric| metric.value().name() == name)
        .map(|value| value.as_usize())
}

fn q_error(estimated: usize, actual: usize) -> f64 {
    let estimated = estimated.max(1) as f64;
    let actual = actual.max(1) as f64;
    (estimated / actual).max(actual / estimated)
}

struct AbortNotifier {
    aborted: AtomicBool,
    msg: String,
}

impl AbortNotifier {
    fn new(msg: impl Display) -> Arc<Self> {
        Arc::new(AbortNotifier {
            aborted: AtomicBool::new(true),
            msg: msg.to_string(),
        })
    }

    fn finished(&self) {
        self.aborted
            .store(false, std::sync::atomic::Ordering::Relaxed)
    }
}

impl Drop for AbortNotifier {
    fn drop(&mut self) {
        if self.aborted.load(std::sync::atomic::Ordering::Relaxed) {
            warn!("{}", self.msg);
        }
    }
}

fn err(s: impl Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, s.to_string())
}

async fn get_self_url() -> Result<Url, Box<dyn Error>> {
    let pod_ip = std::env::var("POD_IP")
        .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "POD_IP is not set"))?;
    Ok(Url::parse(&format!("http://{pod_ip}:9001"))?)
}

#[derive(Clone)]
struct DnsWorkerResolver {
    urls: Arc<RwLock<Vec<Url>>>,
}

async fn background_dns_worker_resolver(urls: Arc<RwLock<Vec<Url>>>, worker_dns_name: String) {
    loop {
        let addresses = match tokio::net::lookup_host((worker_dns_name.as_str(), 9001)).await {
            Ok(addresses) => addresses,
            Err(err) => {
                warn!("Error resolving benchmark workers: {err}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let mut workers = addresses
            .map(|address| Url::parse(&format!("http://{address}")).unwrap())
            .collect::<Vec<_>>();
        workers.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        workers.dedup();
        if !urls.read().unwrap().eq(&workers) {
            info!(
                "New set of workers found: {}",
                workers
                    .iter()
                    .map(|url| url.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            *urls.write().unwrap() = workers;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

impl DnsWorkerResolver {
    fn new(worker_dns_name: String) -> Self {
        let urls = Arc::new(RwLock::new(Vec::new()));
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(background_dns_worker_resolver(
            urls.clone(),
            worker_dns_name,
        ));
        Self { urls }
    }
}

#[async_trait]
impl WorkerResolver for DnsWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.read().unwrap().clone())
    }
}
