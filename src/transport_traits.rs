use anyhow::Result;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{Action, FlightClient, Ticket};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{duplex, DuplexStream};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint, Server, Uri};
use tonic::server::NamedService;
use tonic::{Status};

use crate::transport::{self, WorkerTransport};


pub struct InMemTransport {
    chan: Channel,
}

impl InMemTransport {
    /// Build a transport pair: `(client_half, server_half)` to feed into WorkerDiscovery  
    pub async fn pair<S>(
        svc: S,
    ) -> Result<(Arc<Self>, ())>
    where
        S: FlightService + Send + 'static,
    {
        // 32 KiB buffer is usually enough; adjust if you stream large batches
        let (cli_io, srv_io) = duplex(32 * 1024);

        // 1a. spin up the server on one half
        tokio::spawn(run_flight_server(svc, srv_io));

        // 1b. build a tonic `Channel` over the client half
        let chan = Endpoint::try_from("http://inmem")
            .unwrap()               // URI never used on-wire
            .connect_with_connector(tower::service_fn(move |_: Uri| {
                // each connect() gets its own copy of the duplex half
                async move { Ok::<_, std::io::Error>(cli_io) }
            }))
            .await?;

        Ok((Arc::new(Self { chan }), ()))
    }
}

// implement the WorkerTransport for in memory requests 
#[async_trait]
impl WorkerTransport for InMemTransport {
    async fn do_action(
        &self,
        action: Action,
    ) -> Result<BoxStream<'static, Result<Bytes, Status>>> {
        let mut client = FlightClient::new(self.chan.clone());
        Ok(client.do_action(action).await?)
    }

    async fn do_get(
        &self,
        ticket: Ticket,
    ) -> Result<FlightRecordBatchStream> {
        let mut client = FlightClient::new(self.chan.clone());
        Ok(client.do_get(ticket).await?)
    }
}

// ---------------------------------
// Helper: run a tiny Flight server
// ---------------------------------
async fn run_flight_server<S>(svc: S, io: DuplexStream)
where
    S: Flight + NamedService + 'static,
{
    // tonic lets you serve **one** IO object with `Server::builder().serve_with_incoming_stream`
    // We adapt our single DuplexStream into a stream-of-exactly-one.
    let incoming = ReceiverStream::new(tokio_stream::once(Ok::<_, std::io::Error>(io)));

    Server::builder()
        .add_service(FlightServiceServer::new(svc))
        .serve_with_incoming(incoming)
        .await
        .ok(); // ignore shutdown errors in tests
}


pub struct GrpcTransport {
    chan: Channel,
}

impl GrpcTransport {
    pub async fn connect(addr: &str) -> Result<Arc<Self>> {
        let chan = Channel::from_shared(format!("http://{addr}"))?
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await?;
        Ok(Arc::new(Self { chan }))
    }
}

#[async_trait]
impl WorkerTransport for GrpcTransport { 

    async fn do_action(
        &self,
        action: Action,
    ) -> Result<BoxStream<'static, Result<Bytes, Status>>> {
        let res = self.client.do_action(action).await;
        // if the request fails & we fail to connect, we remove the transport from the registry
        if res.is_err() { transport::poison(&self.host) }
        res.map_err(Into::into)
    }

    async fn do_get(
        &self,
        ticket: Ticket,
    ) -> Result<FlightRecordBatchStream> {
        let res = self.client.do_get(ticket).await;
        if res.is_err() { transport::poison(&self.host) }
        res.map_err(Into::into)
    }
}