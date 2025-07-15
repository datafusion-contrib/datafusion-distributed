use anyhow::Result;
use clap::Parser;
use distributed_datafusion::{
    friendly::new_friendly_name, proxy_service::DDProxyService, setup,
    worker_service::DDWorkerService,
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
            let service = DDProxyService::new(new_friendly_name()?, args.port, None).await?;
            service.serve().await?;
        }
        "worker" => {
            let service = DDWorkerService::new(new_friendly_name()?, args.port).await?;
            service.serve().await?;
        }
        _ => {
            eprintln!("Invalid mode: {}. Must be 'proxy' or 'worker'", args.mode);
            std::process::exit(1);
        }
    }

    Ok(())
}
