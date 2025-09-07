use crate::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedSessionBuilder, DistributedSessionBuilderContext,
    MappedDistributedSessionBuilderExt,
};
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use datafusion::common::runtime::JoinSet;
use datafusion::common::DataFusionError;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};
use url::Url;

pub fn get_free_ports(n: usize) -> Vec<u16> {
    let listeners = (0..n)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .collect::<Result<Vec<_>, _>>()
        .expect("Failed to bind to address");
    listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect()
}

pub async fn start_localhost_context<B>(
    num_workers: usize,
    session_builder: B,
) -> (SessionContext, JoinSet<()>)
where
    B: DistributedSessionBuilder + Send + Sync + 'static,
    B: Clone,
{
    let ports = get_free_ports(num_workers);

    let channel_resolver = LocalHostChannelResolver::new(ports.clone());
    let session_builder = session_builder.map(move |builder: SessionStateBuilder| {
        let channel_resolver = channel_resolver.clone();
        Ok(builder
            .with_distributed_channel_resolver(channel_resolver)
            .build())
    });
    let mut join_set = JoinSet::new();
    for port in ports {
        let session_builder = session_builder.clone();
        let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        join_set.spawn(async move {
            spawn_flight_service(session_builder, listener)
                .await
                .unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut state = session_builder
        .build_session_state(DistributedSessionBuilderContext {
            runtime_env: Arc::new(RuntimeEnv::default()),
            headers: Default::default(),
        })
        .await
        .unwrap();
    state.config_mut().options_mut().execution.target_partitions = 3;

    (SessionContext::from(state), join_set)
}

#[derive(Clone)]
pub struct LocalHostChannelResolver {
    ports: Vec<u16>,
}

impl LocalHostChannelResolver {
    pub fn new<N: TryInto<u16>, I: IntoIterator<Item = N>>(ports: I) -> Self
    where
        N::Error: std::fmt::Debug,
    {
        Self {
            ports: ports.into_iter().map(|v| v.try_into().unwrap()).collect(),
        }
    }
}

#[async_trait]
impl ChannelResolver for LocalHostChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.ports
            .iter()
            .map(|port| format!("http://localhost:{port}"))
            .map(|url| Url::parse(&url).map_err(external_err))
            .collect::<Result<Vec<Url>, _>>()
    }
    async fn get_channel_for_url(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError> {
        let endpoint = Channel::from_shared(url.to_string()).map_err(external_err)?;
        let channel = endpoint.connect().await.map_err(external_err)?;
        Ok(BoxCloneSyncChannel::new(channel))
    }
}

pub async fn spawn_flight_service(
    session_builder: impl DistributedSessionBuilder + Send + Sync + 'static,
    incoming: TcpListener,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let endpoint = ArrowFlightEndpoint::try_new(session_builder)?;

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(incoming);

    Ok(Server::builder()
        .add_service(FlightServiceServer::new(endpoint))
        .serve_with_incoming(incoming)
        .await?)
}

fn external_err(err: impl Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
