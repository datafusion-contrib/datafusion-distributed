use datafusion_distributed::Worker;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;
use tonic::transport::Server;

#[derive(StructOpt)]
#[structopt(name = "localhost_worker", about = "A localhost DataFusion worker")]
struct Args {
    #[structopt(default_value = "8080")]
    port: u16,

    /// Optional version string reported via GetWorkerInfo (e.g. "2.0", a git SHA).
    #[structopt(long)]
    version: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let mut worker = Worker::default();
    if let Some(version) = &args.version {
        worker = worker.with_version(version);
    }

    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port);
    println!("Worker listening on {addr}");

    Server::builder()
        .add_service(worker.into_worker_server())
        .serve(addr)
        .await?;

    Ok(())
}
