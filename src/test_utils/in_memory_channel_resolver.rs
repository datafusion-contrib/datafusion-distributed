use crate::worker::InMemoryWorkerTransport;
use crate::{DistributedExt, SessionStateBuilderExt, Worker, WorkerResolver, WorkerSessionBuilder};
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use url::Url;

#[cfg(feature = "flight")]
use crate::worker::generated::worker::worker_service_client::WorkerServiceClient;
#[cfg(feature = "flight")]
use crate::{
    BoxCloneSyncChannel, ChannelResolver, DefaultSessionBuilder, MappedWorkerSessionBuilderExt,
    create_worker_client,
};
#[cfg(feature = "flight")]
use async_trait::async_trait;
#[cfg(feature = "flight")]
use hyper_util::rt::TokioIo;
#[cfg(feature = "flight")]
use tonic::transport::{Endpoint, Server};

#[cfg(feature = "flight")]
const DUMMY_URL: &str = "http://localhost:50051";
const DUMMY_URL_PREFIX: &str = "http://url-";

/// [ChannelResolver] implementation that returns gRPC clients backed by an in-memory
/// tokio duplex rather than a TCP connection.
#[cfg(feature = "flight")]
#[derive(Clone)]
pub struct InMemoryChannelResolver {
    channel: WorkerServiceClient<BoxCloneSyncChannel>,
}

#[cfg(feature = "flight")]
impl InMemoryChannelResolver {
    /// Build an [InMemoryChannelResolver] with a custom [WorkerSessionBuilder].
    /// This allows you to inject your own DataFusion extensions in the in-memory worker
    /// spawned by this method.
    pub fn from_session_builder(
        builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    ) -> Self {
        Self::from_configured_worker(builder, |worker| worker)
    }

    /// Build an [InMemoryChannelResolver] with a custom [WorkerSessionBuilder] and worker setup.
    pub fn from_configured_worker(
        builder: impl WorkerSessionBuilder + Send + Sync + 'static,
        configure_worker: impl Fn(Worker) -> Worker + Send + Sync + 'static,
    ) -> Self {
        let (client, server) = tokio::io::duplex(1024 * 1024);

        let mut client = Some(client);
        let channel = Endpoint::try_from(DUMMY_URL)
            .expect("Invalid dummy URL for building an endpoint. This should never happen")
            .connect_with_connector_lazy(tower::service_fn(move |_| {
                let client = client
                    .take()
                    .expect("Client taken twice. This should never happen");
                async move { Ok::<_, std::io::Error>(TokioIo::new(client)) }
            }));

        let this = Self {
            channel: create_worker_client(BoxCloneSyncChannel::new(channel)),
        };
        let this_clone = this.clone();

        let endpoint = Worker::from_session_builder(builder.map(move |builder| {
            let this = this.clone();
            Ok(builder.with_distributed_channel_resolver(this).build())
        }));
        let endpoint = configure_worker(endpoint);

        #[allow(clippy::disallowed_methods)]
        tokio::spawn(async move {
            Server::builder()
                .add_service(endpoint.into_worker_server())
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });

        this_clone
    }
}

#[cfg(feature = "flight")]
impl Default for InMemoryChannelResolver {
    fn default() -> Self {
        Self::from_session_builder(DefaultSessionBuilder)
    }
}

#[cfg(feature = "flight")]
#[async_trait]
impl ChannelResolver for InMemoryChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        _: &url::Url,
    ) -> Result<WorkerServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        Ok(self.channel.clone())
    }
}

pub struct InMemoryWorkerResolver {
    n_workers: usize,
}

impl InMemoryWorkerResolver {
    pub fn new(n_workers: usize) -> Self {
        Self { n_workers }
    }
}

impl WorkerResolver for InMemoryWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        (0..self.n_workers)
            .map(|i| Url::parse(&format!("{}{}", DUMMY_URL_PREFIX, i)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| DataFusionError::External(Box::new(err)))
    }
}

#[cfg(feature = "flight")]
/// Creates a distributed session context backed by a single in-memory gRPC worker service. The
/// produced worker URLs are deterministic, taking the form http://url-<i>; routing tests that emit
/// and assert per-URL worker identity need the distinct dialed workers this gives, which the single
/// in-process worker can't represent.
pub async fn start_in_memory_context(
    num_workers: usize,
    session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
) -> SessionContext {
    let channel_resolver = InMemoryChannelResolver::from_session_builder(session_builder);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_planner()
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(num_workers))
        .with_distributed_channel_resolver(channel_resolver)
        .build();
    SessionContext::from(state)
}

/// Creates a distributed session context backed by a configurable in-process worker.
///
/// Like [crate::test_utils::localhost::start_localhost_context], this uses tiny file-scan
/// partitions so small test datasets still cross worker boundaries.
pub async fn start_configured_in_memory_context(
    num_workers: usize,
    session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    configure_worker: impl Fn(Worker) -> Worker + Send + Sync + 'static,
) -> SessionContext {
    let transport =
        InMemoryWorkerTransport::from_configured_worker(session_builder, configure_worker);
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_target_partitions(num_workers))
        .with_distributed_planner()
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(num_workers))
        .with_distributed_worker_transport(transport)
        .with_distributed_file_scan_config_bytes_per_partition(1)
        .unwrap()
        .build();
    SessionContext::from(state)
}
