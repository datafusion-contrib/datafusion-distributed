use arrow_flight::flight_service_client::FlightServiceClient;
use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_ec2::Client as Ec2Client;
use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
use dashmap::{DashMap, Entry};
use datafusion::common::DataFusionError;
use datafusion::common::instant::Instant;
use datafusion::common::runtime::SpawnedTask;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedPhysicalOptimizerRule, DistributedSessionBuilder, DistributedSessionBuilderContext,
    create_flight_client, display_plan_ascii,
};
use futures::{StreamExt, TryFutureExt};
use log::{error, info, warn};
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use structopt::StructOpt;
use tonic::transport::{Channel, Server};
use url::Url;

#[derive(Serialize)]
struct QueryResult {
    plan: String,
    count: usize,
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(about = "worker spawn command")]
struct Cmd {
    /// The bucket name.
    #[structopt(long, default_value = "datafusion-distributed-benchmarks")]
    bucket: String,
}

#[derive(Clone)]
struct BenchSessionStateBuilder {
    s3_url: Url,
    s3: Arc<dyn ObjectStore>,
    channel_resolver: Ec2ChannelResolver,
}

impl BenchSessionStateBuilder {
    fn new(s3_url: Url) -> Result<Self, Box<dyn Error>> {
        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(s3_url.host().unwrap().to_string())
            .build()?;
        Ok(Self {
            s3_url,
            s3: Arc::new(s3),
            channel_resolver: Ec2ChannelResolver::new(),
        })
    }
}

#[async_trait]
impl DistributedSessionBuilder for BenchSessionStateBuilder {
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_runtime_env(ctx.runtime_env)
            .with_object_store(&self.s3_url, Arc::clone(&self.s3))
            .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
            .with_distributed_channel_resolver(self.channel_resolver.clone())
            .build();
        Ok(state)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let cmd = Cmd::from_args();

    const LISTENER_ADDR: &str = "0.0.0.0:9000";
    const WORKER_ADDR: &str = "0.0.0.0:9001";

    info!("Starting HTTP listener on {LISTENER_ADDR}...");
    let listener = tokio::net::TcpListener::bind(LISTENER_ADDR).await?;

    // Register S3 object store
    let s3_url = Url::parse(&format!("s3://{}", cmd.bucket))?;
    let state_builder = BenchSessionStateBuilder::new(s3_url)?;

    info!("Building shared SessionContext for the whole lifetime of the HTTP listener...");
    let state = state_builder
        .build_session_state(Default::default())
        .await?;
    let ctx = SessionContext::from(state);

    let arrow_flight_endpoint = ArrowFlightEndpoint::try_new(state_builder.clone())?;
    let http_server = axum::serve(
        listener,
        Router::new().route(
            "/",
            get(move |Query(params): Query<HashMap<String, String>>| {
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
                    let plan = display_plan_ascii(physical.as_ref(), true);
                    drop(task);

                    let elapsed = start.elapsed();
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    info!("Finished executing query:\n{sql}\n\n{plan}");
                    info!("Returned {count} rows in {ms} ms");
                    abort_notifier.finished();

                    Ok::<_, (StatusCode, String)>(Json(QueryResult { count, plan }))
                }
                .inspect_err(|(_, msg)| {
                    error!("Error executing query: {msg}");
                })
            }),
        ),
    );
    let grpc_server = Server::builder()
        .add_service(arrow_flight_endpoint.into_flight_server())
        .serve(WORKER_ADDR.parse()?);

    info!("Started listener HTTP server in {LISTENER_ADDR}");
    info!("Started distributed DataFusion worker in {WORKER_ADDR}");

    tokio::select! {
        result = http_server => result?,
        result = grpc_server => result?,
    }

    Ok(())
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

#[derive(Clone)]
struct Ec2ChannelResolver {
    urls: Arc<RwLock<Vec<Url>>>,
    channels: Arc<DashMap<Url, BoxCloneSyncChannel>>,
}

async fn background_ec2_worker_resolver(urls: Arc<RwLock<Vec<Url>>>) {
    tokio::spawn(async move {
        let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
        let ec2_client = Ec2Client::new(&config);

        loop {
            let result = match ec2_client
                .describe_instances()
                .filters(
                    aws_sdk_ec2::types::Filter::builder()
                        .name("tag:BenchmarkCluster")
                        .values("datafusion")
                        .build(),
                )
                .filters(
                    aws_sdk_ec2::types::Filter::builder()
                        .name("instance-state-name")
                        .values("running")
                        .build(),
                )
                .send()
                .await
            {
                Ok(v) => v,
                Err(err) => {
                    eprintln!("Error discovering workers: {}", err.into_service_error());
                    continue;
                }
            };

            let mut workers = Vec::new();
            for reservation in result.reservations() {
                for instance in reservation.instances() {
                    if let Some(private_ip) = instance.private_ip_address() {
                        let url = Url::parse(&format!("http://{private_ip}:9001")).unwrap();
                        workers.push(url);
                    }
                }
            }
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
    });
}

impl Ec2ChannelResolver {
    fn new() -> Self {
        let urls = Arc::new(RwLock::new(Vec::new()));
        let channels = Arc::new(DashMap::new());
        tokio::spawn(background_ec2_worker_resolver(urls.clone()));
        Self { urls, channels }
    }
}

#[async_trait]
impl ChannelResolver for Ec2ChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.read().unwrap().clone())
    }

    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        let channel = match self.channels.entry(url.clone()) {
            Entry::Occupied(v) => v.get().clone(),
            Entry::Vacant(v) => {
                let endpoint = Channel::from_shared(url.to_string()).unwrap();
                let channel = endpoint.connect_lazy();
                let channel = BoxCloneSyncChannel::new(channel);
                v.insert(channel.clone());
                channel
            }
        };
        Ok(create_flight_client(channel))
    }
}
