use crate::backend::BenchmarkBackend;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::Result;
use datafusion_distributed::test_utils::work_unit_file_scan::WorkUnitFileScanCodec;
use datafusion_distributed::{DistributedExt, Worker, WorkerQueryContext};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::codegen::tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Localhost workers running in the current process.
///
/// Workers are spawned on the benchmark runner's own Tokio runtime rather than getting one
/// runtime each, so the process has a single pool of scheduler threads and a single blocking
/// pool, and profilers attached to it observe the coordinator and the workers on the same few
/// threads.
pub(crate) struct LocalWorkers {
    /// Aborted when dropped, which stops the workers.
    tasks: Vec<SpawnedTask<()>>,
    shutdown: watch::Sender<()>,
    pub(crate) workers: Vec<Worker>,
    pub(crate) ports: Vec<u16>,
}

impl LocalWorkers {
    /// Starts `count` workers on the current Tokio runtime, listening on OS-assigned localhost
    /// ports.
    pub(crate) async fn start<B: BenchmarkBackend + Clone>(
        count: usize,
        backend: &B,
    ) -> Result<Self> {
        let (shutdown, shutdown_rx) = watch::channel(());
        let mut tasks = Vec::with_capacity(count);
        let mut ports = Vec::with_capacity(count);
        let mut workers = Vec::with_capacity(count);
        for i in 0..count {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            ports.push(listener.local_addr()?.port());
            let backend = backend.clone();
            let mut shutdown_rx = shutdown_rx.clone();
            let signal = async move {
                let _ = shutdown_rx.changed().await;
            };

            // Workers need to be able to decode the WorkUnitFileScan plan
            // node when the feature is on. The codec is registered via a
            // session builder so it is installed on every worker session.
            let worker = Worker::from_session_builder(move |ctx: WorkerQueryContext| {
                let builder = backend.configure_session(
                    ctx.builder
                        .with_distributed_user_codec(WorkUnitFileScanCodec),
                );
                async move { Ok(builder.build()) }
            });
            workers.push(worker.clone());

            let server = Server::builder().add_service(worker.into_worker_server());
            tasks.push(SpawnedTask::spawn(async move {
                let result = server
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), signal)
                    .await;
                if let Err(err) = result {
                    eprintln!("Local worker {i} failed: {err}");
                }
            }));
        }
        Ok(Self {
            tasks,
            workers,
            shutdown,
            ports,
        })
    }

    /// Gracefully stops the workers, waiting for them to close their connections.
    ///
    /// Every connection has both ends in this process. Without this, which end closes first at
    /// exit is arbitrary, and connections closed by the client end leave a `TIME_WAIT` entry on
    /// an ephemeral port for up to a minute. With many workers, back-to-back runs can then run
    /// out of ephemeral ports and fail with `EADDRNOTAVAIL`. Closing from the server end keeps
    /// those entries on the workers' listener ports instead.
    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(());
        for task in self.tasks {
            let _ = task.join().await;
        }
    }
}
