#[cfg(feature = "flight")]
use crate::WorkerResolver;
use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
use crate::worker::InMemoryWorkerTransport;
use crate::{DistributedExt, SessionStateBuilderExt, Worker, WorkerSessionBuilder};
#[cfg(feature = "flight")]
use async_trait::async_trait;
#[cfg(feature = "flight")]
use datafusion::common::DataFusionError;
use datafusion::common::runtime::JoinSet;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
#[cfg(feature = "flight")]
use std::error::Error;
#[cfg(feature = "flight")]
use std::time::Duration;
#[cfg(feature = "flight")]
use tokio::net::TcpListener;
#[cfg(feature = "flight")]
use tonic::transport::Server;
#[cfg(feature = "flight")]
use url::Url;

/// Create workers and context on localhost with a fixed number of target partitions, behind the
/// Arrow-Flight gRPC transport. For flight-specific tests (network metrics, URL routing); the
/// generic suite runs through [start_localhost_context] instead.
///
/// Creates `num_workers` listeners, all bound to a random OS decided port on `127.0.0.1`, then
/// attaches a channel resolver that is aware of these addresses to `session_builder` and uses it
/// to spawn a flight service behind each listener.
///
/// Returns a session context aware of these workers, and a join set of all spawned worker tasks.
#[cfg(feature = "flight")]
pub async fn start_localhost_flight_context<B>(
    num_workers: usize,
    session_builder: B,
) -> (SessionContext, JoinSet<()>, Vec<Worker>)
where
    B: WorkerSessionBuilder + Send + Sync + 'static,
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

    let mut join_set = JoinSet::new();
    let mut workers = vec![];
    for listener in listeners {
        let session_builder = session_builder.clone();
        let worker = Worker::from_session_builder(session_builder);
        workers.push(worker.clone());

        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

        join_set.spawn(async move {
            Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let worker_resolver = LocalHostWorkerResolver::new(ports);
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_planner()
        .with_distributed_worker_resolver(worker_resolver)
        // Test datasets are tiny, so budget one byte per partition: the estimator then asks for far
        // more partitions than exist, capped at the worker count, fanning every scan across the
        // whole (small) test cluster so the distributed paths are exercised.
        .with_distributed_file_scan_config_bytes_per_partition(1)
        .unwrap()
        .build();
    state.config_mut().options_mut().execution.target_partitions = 3;

    (SessionContext::from(state), join_set, workers)
}

/// Workers and context with a fixed number of target partitions, hosted in-process by an
/// [InMemoryWorkerTransport] built from `session_builder`. Plans are delivered with a function call
/// and partitions are read straight from the local registry, so this is what the integration suite
/// exercises by default in both build configurations. Nothing listens on localhost; the returned
/// [Worker]s are handles onto the shared in-process task registry.
pub async fn start_localhost_context<B>(
    num_workers: usize,
    session_builder: B,
) -> (SessionContext, JoinSet<()>, Vec<Worker>)
where
    B: WorkerSessionBuilder + Send + Sync + 'static,
    B: Clone,
{
    // CI runs this same suite over Flight as a separate job, so both transports get full coverage.
    #[cfg(feature = "flight")]
    if std::env::var("DATAFUSION_DISTRIBUTED_TEST_TRANSPORT").as_deref() == Ok("flight") {
        return start_localhost_flight_context(num_workers, session_builder).await;
    }
    let transport = InMemoryWorkerTransport::from_session_builder(session_builder);
    let workers = (0..num_workers)
        .map(|_| transport.worker().clone())
        .collect();

    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_distributed_planner()
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(num_workers))
        .with_distributed_worker_transport(transport)
        // Tiny test datasets: budget one byte per partition so the estimator fans scans out across
        // the whole (small) cluster and the distributed paths actually run.
        .with_distributed_file_scan_config_bytes_per_partition(1)
        .unwrap()
        .build();
    state.config_mut().options_mut().execution.target_partitions = 3;

    (SessionContext::from(state), JoinSet::new(), workers)
}

#[cfg(feature = "flight")]
#[derive(Clone)]
pub struct LocalHostWorkerResolver {
    ports: Vec<u16>,
}

#[cfg(feature = "flight")]
impl LocalHostWorkerResolver {
    pub fn new<N: TryInto<u16>, I: IntoIterator<Item = N>>(ports: I) -> Self
    where
        N::Error: std::fmt::Debug,
    {
        Self {
            ports: ports.into_iter().map(|v| v.try_into().unwrap()).collect(),
        }
    }
}

#[cfg(feature = "flight")]
#[async_trait]
impl WorkerResolver for LocalHostWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        self.ports
            .iter()
            .map(|port| format!("http://localhost:{port}"))
            .map(|url| Url::parse(&url).map_err(external_err))
            .collect::<Result<Vec<Url>, _>>()
    }
}

#[cfg(feature = "flight")]
pub async fn spawn_worker_service(
    session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    incoming: TcpListener,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let endpoint = Worker::from_session_builder(session_builder);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(incoming);

    Ok(Server::builder()
        .add_service(endpoint.into_worker_server())
        .serve_with_incoming(incoming)
        .await?)
}

#[cfg(feature = "flight")]
fn external_err(err: impl Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
