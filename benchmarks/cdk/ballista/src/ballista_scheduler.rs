use ballista::datafusion::execution::runtime_env::RuntimeEnvBuilder;
use ballista::datafusion::execution::{SessionState, SessionStateBuilder};
use ballista::datafusion::prelude::SessionConfig;
use ballista_core::error::BallistaError;
use ballista_core::extension::SessionConfigExt;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{Config, SchedulerConfig};
use ballista_scheduler::scheduler_process::start_server;
use clap::Parser;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;
use url::Url;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .thread_stack_size(32 * 1024 * 1024) // 32MB
        .build()?;

    runtime.block_on(inner())
}

async fn inner() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Config::parse();

    let addr = format!("{}:{}", opt.bind_host, opt.bind_port);
    let addr = addr
        .parse()
        .map_err(|e: std::net::AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let bucket = env::var("BUCKET").unwrap_or("datafusion-distributed-benchmarks".to_string());
    let region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());

    log::info!("Initializing S3 object store for bucket={bucket}, region={region}");

    let s3_url = Url::parse(&format!("s3://{bucket}"))?;
    let bucket_name = s3_url.host().unwrap().to_string();

    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(&bucket_name)
            .with_region(&region)
            .build()?,
    );

    log::info!("S3 store built successfully for s3://{bucket_name}");

    let config: SchedulerConfig = opt.try_into()?;
    let config = config.with_override_config_producer(Arc::new(|| {
        SessionConfig::new_with_ballista().with_information_schema(true)
    }));
    // Build one RuntimeEnv, register S3, then reuse it across all sessions.
    let runtime_env = RuntimeEnvBuilder::new()
        .build_arc()
        .map_err(|e| ballista::datafusion::common::DataFusionError::External(e.into()))?;
    runtime_env.register_object_store(&s3_url, Arc::clone(&s3));
    log::info!("RuntimeEnv built with S3 registered at {s3_url}");

    let config = config.with_override_session_builder(Arc::new(
        move |cfg: SessionConfig| -> ballista::datafusion::common::Result<SessionState> {
            Ok(SessionStateBuilder::new()
                .with_config(cfg)
                .with_runtime_env(Arc::clone(&runtime_env))
                .with_default_features()
                .build())
        },
    ));

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server(cluster, addr, Arc::new(config)).await?;

    Ok(())
}
