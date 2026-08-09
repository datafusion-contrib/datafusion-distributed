use ballista::datafusion::execution::runtime_env::RuntimeEnvBuilder;
use ballista::datafusion::prelude::SessionConfig;
use ballista_executor::config::Config;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use clap::Parser;
use object_store::aws::AmazonS3Builder;
use std::env;
use std::sync::Arc;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let opt = Config::parse();
    let mut config: ExecutorProcessConfig = opt.try_into()?;

    let bucket = env::var("BUCKET").unwrap_or("datafusion-distributed-benchmarks".to_string());
    let region = env::var("AWS_REGION")
        .or_else(|_| env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());

    log::info!("Initializing S3 object store for bucket={bucket}, region={region}");

    let s3_url = Url::parse(&format!("s3://{bucket}"))?;
    let bucket_name = s3_url.host().unwrap().to_string();

    // Build the S3 store once; it will be Arc-cloned into each task's RuntimeEnv.
    let s3: Arc<dyn object_store::ObjectStore> = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(&bucket_name)
            .with_region(&region)
            .build()?,
    );

    log::info!("S3 store built successfully for s3://{bucket_name}");

    let work_dir = config
        .work_dir
        .clone()
        .unwrap_or_else(|| "/var/ballista/executor".to_string());

    // Build one RuntimeEnv, register S3, then reuse it across all sessions so
    // caches stored at the RuntimeEnv level (e.g. parquet footer cache) are shared.
    let runtime = RuntimeEnvBuilder::new()
        .with_temp_file_path(&work_dir)
        .build_arc()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    runtime.register_object_store(&s3_url, Arc::clone(&s3));
    log::info!("RuntimeEnv built with S3 registered at {s3_url}");

    config.override_runtime_producer = Some(Arc::new(move |_: &SessionConfig| {
        Ok(Arc::clone(&runtime))
    }));

    log::info!("Starting executor process...");
    start_executor_process(Arc::new(config)).await?;
    Ok(())
}
