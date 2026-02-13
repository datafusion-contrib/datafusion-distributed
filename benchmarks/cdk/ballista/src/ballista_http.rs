use axum::{Json, Router, extract::Query, http::StatusCode, routing::get};
use ballista::datafusion::common::instant::Instant;
use ballista::datafusion::execution::SessionStateBuilder;
use ballista::datafusion::execution::runtime_env::RuntimeEnv;
use ballista::datafusion::physical_plan::displayable;
use ballista::datafusion::physical_plan::execute_stream;
use ballista::datafusion::prelude::SessionConfig;
use ballista::datafusion::prelude::SessionContext;
use ballista::prelude::*;
use futures::{StreamExt, TryFutureExt};
use log::{error, info};
use object_store::aws::AmazonS3Builder;
use serde::Serialize;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Display;
use std::sync::Arc;
use structopt::StructOpt;
use url::Url;

#[derive(Serialize)]
struct QueryResult {
    plan: String,
    count: usize,
    elapsed_ms: usize,
}

#[derive(Debug, StructOpt, Clone)]
#[structopt(about = "worker spawn command")]
struct Cmd {
    /// The bucket name.
    #[structopt(long, default_value = "datafusion-distributed-benchmarks")]
    bucket: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .parse_default_env()
        .init();

    let cmd = Cmd::from_args();

    const LISTENER_ADDR: &str = "0.0.0.0:9002";

    info!("Starting HTTP listener on {LISTENER_ADDR}...");
    let listener = tokio::net::TcpListener::bind(LISTENER_ADDR).await?;

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

    let config = SessionConfig::new_with_ballista().with_ballista_job_name("Benchmarks");

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_runtime_env(Arc::clone(&runtime_env))
        .build();
    let ctx = SessionContext::remote_with_state("df://localhost:50050", state).await?;

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
                    let physical = df.create_physical_plan().await.map_err(err)?;
                    let mut stream =
                        execute_stream(physical.clone(), ctx.task_ctx()).map_err(err)?;
                    let mut count = 0;
                    while let Some(batch) = stream.next().await {
                        count += batch.map_err(err)?.num_rows();
                        info!("Gathered {count} rows, query still in progress..")
                    }
                    let plan = displayable(physical.as_ref()).indent(true).to_string();
                    let elapsed = start.elapsed();
                    let ms = elapsed.as_secs_f64() * 1000.0;
                    info!("Returned {count} rows in {ms} ms");

                    Ok::<_, (StatusCode, String)>(Json(QueryResult {
                        count,
                        plan,
                        elapsed_ms: ms,
                    }))
                }
                .inspect_err(|(_, msg)| {
                    error!("Error executing query: {msg}");
                })
            }),
        ),
    );

    info!("Started listener HTTP server in {LISTENER_ADDR}");
    http_server.await?;
    Ok(())
}

fn err(s: impl Display) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, s.to_string())
}
