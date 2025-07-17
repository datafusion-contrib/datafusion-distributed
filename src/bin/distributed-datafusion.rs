use anyhow::Result;
use clap::Parser;
use distributed_datafusion::{
    friendly::new_friendly_name, proxy_service::DDProxyService, setup,
    worker_service::DDWorkerService, worker_discovery::{EnvDiscovery, WorkerDiscovery},
};

#[derive(Parser)]
#[command(name = "distributed-datafusion")]
#[command(about = "A distributed execution engine for DataFusion", long_about = None)]
struct Args {
    /// Port number for the service to listen on
    #[arg(short, long, default_value_t = 20200)]
    port: usize,
    /// mode of the service, either 'proxy' or 'worker'
    #[arg(short, long, default_value = "proxy")]
    mode: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // install a default crypto provider for rustls
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| {
            anyhow::anyhow!("could not install aws_lc_rs as the default CryptoProvider")
        })?;

    // our own setup
    setup();

    let args = Args::parse();
    
    match args.mode.as_str() {
        "proxy" => {
            // TODO: put the k8s or ENV decision behind some flag, WARNING: this will kick the discovery so workers should be up
            let discovery: Arc<dyn WorkerDiscovery> = if std::env::var("DD_WORKER_ADDRESSES").is_ok() {
                Arc::new(EnvDiscovery::new().await?)
            } else {
                Arc::new(K8sDiscovery::new().await?)
            };
            let service = DDProxyService::new(new_friendly_name()?, args.port,discovery, None).await?;
            service.serve().await?;
        }
        "worker" => {
            let service = DDWorkerService::new(new_friendly_name()?, args.port, None).await?;
            service.serve().await?;
        }
        _ => {
            eprintln!("Invalid mode: {}. Must be 'proxy' or 'worker'", args.mode);
            std::process::exit(1);
        }
    }

    Ok(())
}
