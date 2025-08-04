use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use datafusion::common::runtime::JoinSet;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelManager, ChannelResolver, SessionBuilder,
};
use std::error::Error;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Server};
use url::Url;

#[derive(Debug, Clone)]
pub struct NoopSessionBuilder;
impl SessionBuilder for NoopSessionBuilder {
    fn on_new_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder {
        builder
    }
}

pub async fn start_localhost_context<N, I, B>(
    ports: I,
    session_builder: B,
) -> (SessionContext, JoinSet<()>)
where
    N::Error: std::fmt::Debug,
    N: TryInto<u16>,
    I: IntoIterator<Item = N>,
    B: SessionBuilder + Send + Sync + 'static,
    B: Clone,
{
    let ports: Vec<u16> = ports.into_iter().map(|x| x.try_into().unwrap()).collect();
    let channel_resolver = LocalHostChannelResolver::new(ports.clone());
    let mut join_set = JoinSet::new();
    for port in ports {
        let channel_resolver = channel_resolver.clone();
        let session_builder = session_builder.clone();
        join_set.spawn(async move {
            spawn_flight_service(channel_resolver, session_builder, port)
                .await
                .unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let ctx = SessionContext::new();

    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(ChannelManager::new(channel_resolver)));

    (ctx, join_set)
}

#[derive(Clone)]
pub struct LocalHostChannelResolver {
    ports: Vec<u16>,
    i: Arc<AtomicUsize>,
}

impl LocalHostChannelResolver {
    pub fn new<N: TryInto<u16>, I: IntoIterator<Item = N>>(ports: I) -> Self
    where
        N::Error: std::fmt::Debug,
    {
        Self {
            i: Arc::new(AtomicUsize::new(0)),
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
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
    session_builder: impl SessionBuilder + Send + Sync + 'static,
    port: u16,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut endpoint = ArrowFlightEndpoint::new(channel_resolver);
    endpoint.with_session_builder(session_builder);
    Ok(Server::builder()
        .add_service(FlightServiceServer::new(endpoint))
        .serve(format!("127.0.0.1:{port}").parse()?)
        .await?)
}

fn external_err(err: impl Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
