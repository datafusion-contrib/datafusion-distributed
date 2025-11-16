use async_trait::async_trait;
use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
use datafusion::common::DataFusionError;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    ArrowFlightEndpoint, DistributedExt, DistributedPhysicalOptimizerRule,
    DistributedSessionBuilder, DistributedSessionBuilderContext, display_plan_ascii,
};
use datafusion_distributed_benchmarks::StaticChannelResolver;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;
use structopt::StructOpt;
use tonic::transport::Server;
use url::Url;

#[derive(Serialize)]
struct QueryResult {
    plan: String,
    count: usize,
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(about = "worker spawn command")]
struct Worker {
    /// The Urls of all the workers involved in the query.
    #[structopt(long, use_delimiter = true)]
    workers: Vec<String>,
    /// The bucket name.
    #[structopt(long, default_value = "datafusion-distributed-benchmarks")]
    bucket: String,
}

#[derive(Clone)]
struct BenchSessionStateBuilder {
    workers: Vec<String>,
    s3_url: Url,
    s3: Arc<dyn ObjectStore>,
}

impl BenchSessionStateBuilder {
    fn new(s3_url: Url, workers: Vec<String>) -> Result<Self, Box<dyn Error>> {
        let s3 = AmazonS3Builder::from_env()
            .with_bucket_name(s3_url.host().unwrap().to_string())
            .build()?;
        Ok(Self {
            workers,
            s3_url,
            s3: Arc::new(s3),
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
            .with_distributed_channel_resolver(StaticChannelResolver::new(&self.workers))
            .build();
        Ok(state)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    let cmd = Worker::from_args();

    const LISTENER_ADDR: &str = "0.0.0.0:8000";
    const WORKER_ADDR: &str = "0.0.0.0:8001";

    let listener = tokio::net::TcpListener::bind(LISTENER_ADDR).await?;

    // Register S3 object store
    let s3_url = Url::parse(&format!("s3://{}", cmd.bucket))?;
    let state_builder = BenchSessionStateBuilder::new(s3_url, cmd.workers.clone())?;

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

                    let physical = df.create_physical_plan().await.map_err(err)?;
                    let stream = execute_stream(physical.clone(), ctx.task_ctx()).map_err(err)?;
                    let batches = stream.try_collect::<Vec<_>>().await.map_err(err)?;
                    let count = batches.iter().map(|b| b.num_rows()).sum::<usize>();
                    let plan = display_plan_ascii(physical.as_ref(), true);

                    Ok::<_, (StatusCode, String)>(Json(QueryResult { count, plan }))
                }
            }),
        ),
    );
    let grpc_server = Server::builder()
        .add_service(arrow_flight_endpoint.into_flight_server())
        .serve(WORKER_ADDR.parse()?);

    println!("Started listener http server in {LISTENER_ADDR}");
    println!("Started distributed DataFusion worker in {WORKER_ADDR}");

    tokio::select! {
        result = http_server => result?,
        result = grpc_server => result?,
    }

    Ok(())
}

fn err(s: impl Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, s.to_string())
}
