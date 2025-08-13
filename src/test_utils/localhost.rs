use crate::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelManager, ChannelResolver, SessionBuilder,
};
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion::{common::runtime::JoinSet, prelude::SessionConfig};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};
use url::Url;

pub async fn start_localhost_context<B>(
    num_workers: usize,
    session_builder: B,
) -> (SessionContext, JoinSet<()>)
where
    B: SessionBuilder + Send + Sync + 'static,
    B: Clone,
{
    let listeners = futures::future::try_join_all(
        (0..num_workers)
            .map(|_| TcpListener::bind("127.0.0.1:0"))
            .collect::<Vec<_>>(),
    )
    .await
    .expect("Failed to bind to address");

    let ports: Vec<u16> = listeners
        .iter()
        .map(|listener| {
            listener
                .local_addr()
                .expect("Failed to get local address")
                .port()
        })
        .collect();

    let channel_resolver = LocalHostChannelResolver::new(ports.clone());
    let mut join_set = JoinSet::new();
    for listener in listeners {
        let channel_resolver = channel_resolver.clone();
        let session_builder = session_builder.clone();
        join_set.spawn(async move {
            spawn_flight_service(channel_resolver, session_builder, listener)
                .await
                .unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let config = SessionConfig::new().with_target_partitions(3);

    let builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config);
    let builder = session_builder.session_state_builder(builder).unwrap();

    let state = builder.build();
    let state = session_builder.session_state(state).await.unwrap();

    let ctx = SessionContext::new_with_state(state);
    let ctx = session_builder.session_context(ctx).await.unwrap();

    ctx.state_ref()
        .write()
        .config_mut()
        .set_extension(Arc::new(ChannelManager::new(channel_resolver)));

    (ctx, join_set)
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
    channel_resolver: impl ChannelResolver + Send + Sync + 'static,
    session_builder: impl SessionBuilder + Send + Sync + 'static,
    incoming: TcpListener,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut endpoint = ArrowFlightEndpoint::new(channel_resolver);
    endpoint.with_session_builder(session_builder);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(incoming);

    Ok(Server::builder()
        .add_service(FlightServiceServer::new(endpoint))
        .serve_with_incoming(incoming)
        .await?)
}

fn external_err(err: impl Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
