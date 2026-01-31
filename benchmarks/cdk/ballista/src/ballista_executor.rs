use ballista::datafusion::execution::runtime_env::RuntimeEnv;
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
    let opt = Config::parse();

    let mut config: ExecutorProcessConfig = opt.try_into()?;

    let bucket = env::var("BUCKET").unwrap_or("datafusion-distributed-benchmarks".to_string());
    let s3_url = Url::parse(&format!("s3://{bucket}"))?;

    let s3 = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(s3_url.host().unwrap().to_string())
            .build()?,
    );
    let runtime_env = Arc::new(RuntimeEnv::default());
    runtime_env.register_object_store(&s3_url, s3);

    config.override_runtime_producer = Some(Arc::new(
        move |_: &SessionConfig| -> ballista::datafusion::common::Result<Arc<RuntimeEnv>> {
            Ok(runtime_env.clone())
        },
    ));

    start_executor_process(Arc::new(config)).await?;
    Ok(())
}
