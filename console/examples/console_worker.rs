use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion_distributed::{DEFAULT_WORKER_PORT, Worker, WorkerResolver};
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use structopt::StructOpt;
use tonic::transport::Server;
use url::Url;

#[derive(StructOpt)]
#[structopt(
    name = "console_worker",
    about = "A localhost DataFusion worker with observability"
)]
struct Args {
    #[structopt(default_value = DEFAULT_WORKER_PORT)]
    port: u16,

    /// The ports holding Distributed DataFusion workers.
    #[structopt(long = "cluster-ports", use_delimiter = true)]
    cluster_ports: Vec<u16>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();
    let localhost_resolver = LocalhostWorkerResolver {
        ports: args.cluster_ports.clone(),
    };
    let worker = Worker::default();

    Server::builder()
        .add_service(worker.with_observability_service(localhost_resolver))
        .add_service(worker.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), args.port))
        .await?;

    Ok(())
}

#[derive(Clone)]
struct LocalhostWorkerResolver {
    ports: Vec<u16>,
}

#[async_trait]
impl WorkerResolver for LocalhostWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.ports
            .iter()
            .map(|port| {
                let url_string = format!("http://localhost:{port}");
                Url::parse(&url_string).map_err(|e| DataFusionError::External(Box::new(e)))
            })
            .collect::<Result<Vec<Url>, _>>()
    }
}
