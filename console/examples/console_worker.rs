use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion_distributed::{ObservabilityServiceServer, SharedWorker};
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

    let worker = SharedWorker::default();
    let flight_service = FlightServiceServer::new(worker.clone());
    let observability_service = ObservabilityServiceServer::new(worker.clone());

    Server::builder()
        .add_service(flight_service)
        .add_service(observability_service)
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port))
        .await?;

    Ok(())
}
