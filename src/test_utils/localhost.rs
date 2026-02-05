use crate::{
    DistributedExt, DistributedPhysicalOptimizerRule, Worker, WorkerResolver, WorkerSessionBuilder,
};
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::common::runtime::JoinSet;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;
use url::Url;

/// Create workers and context on localhost with a fixed number of target partitions.
///
/// Creates `num_workers` listeners, all bound to a random OS decided port on `127.0.0.1`, then
/// attaches a channel resolver that is aware of these addresses to `session_builder` and uses it
/// to spawn a flight service behind each listener.
///
/// Returns a session context aware of these workers, and a join set of all spawned worker tasks.
pub async fn start_localhost_context<B>(
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
                .add_service(worker.into_flight_server())
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    let worker_resolver = LocalHostWorkerResolver::new(ports);
    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule))
        .with_distributed_worker_resolver(worker_resolver)
        .build();
    state.config_mut().options_mut().execution.target_partitions = 3;

    (SessionContext::from(state), join_set, workers)
}

#[derive(Clone)]
pub struct LocalHostWorkerResolver {
    ports: Vec<u16>,
}

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

pub async fn spawn_flight_service(
    session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    incoming: TcpListener,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let endpoint = Worker::from_session_builder(session_builder);

    let incoming = tokio_stream::wrappers::TcpListenerStream::new(incoming);

    Ok(Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve_with_incoming(incoming)
        .await?)
}

fn external_err(err: impl Error + Send + Sync + 'static) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
