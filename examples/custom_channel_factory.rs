//! Connect a logical worker URL through an application-supplied TCP connector.

use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{config_datafusion_err, exec_datafusion_err};
use datafusion_distributed::grpc::{BoxCloneSyncChannel, DefaultChannelResolver};
use datafusion_distributed::{ChannelResolver, GetWorkerInfoRequest, Worker};
use hyper_util::rt::TokioIo;
use std::error::Error;
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tonic::transport::{Endpoint, Server};
use tower::service_fn;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let _server = SpawnedTask::spawn(async move {
        Server::builder()
            .add_service(
                Worker::default()
                    .with_version("custom-connector")
                    .into_worker_server(),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("example worker server failed");
    });

    let resolver = DefaultChannelResolver::with_channel_factory(move |url: Url| async move {
        println!("Opening connection for {url}");
        let endpoint = Endpoint::from_shared(url.to_string())
            .map_err(|error| config_datafusion_err!("Invalid worker endpoint: {error}"))?;
        let channel = endpoint
            .connect_with_connector(service_fn(move |_| async move {
                let tcp = TcpStream::connect(address).await?;
                tcp.set_nodelay(true)?;
                Ok::<_, io::Error>(TokioIo::new(tcp))
            }))
            .await
            .map_err(|error| exec_datafusion_err!("Custom worker connection failed: {error}"))?;
        Ok(BoxCloneSyncChannel::new(channel))
    });

    let logical_url = Url::parse(&format!("http://worker.example:{}", address.port()))?;
    for _ in 0..2 {
        let mut client = resolver.get_worker_client_for_url(&logical_url).await?;
        let info = client.get_worker_info(GetWorkerInfoRequest {}).await?;
        println!("Worker version: {}", info.version);
    }
    Ok(())
}
