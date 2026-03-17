use datafusion_distributed::Worker;
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;
use tonic::transport::Server;

#[derive(StructOpt)]
#[structopt(
    name = "console_worker",
    about = "A localhost DataFusion worker with observability"
)]
struct Args {
    #[structopt(default_value = "8080")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let worker = Worker::default();

    Server::builder()
        .add_service(worker.with_observability_service())
        .add_service(worker.into_worker_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port))
        .await?;

    Ok(())
}
