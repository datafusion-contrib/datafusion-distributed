use datafusion_distributed::{ArrowFlightEndpoint, DistributedSessionBuilderContext};
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;
use tonic::transport::Server;

#[derive(StructOpt)]
#[structopt(name = "localhost_worker", about = "A localhost DataFusion worker")]
struct Args {
    #[structopt(default_value = "8080")]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    let endpoint =
        ArrowFlightEndpoint::try_new(move |ctx: DistributedSessionBuilderContext| async move {
            Ok(ctx.builder.build())
        })?;

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port))
        .await?;

    Ok(())
}
