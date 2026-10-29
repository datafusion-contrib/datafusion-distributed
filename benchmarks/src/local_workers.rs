use crate::backend::BenchmarkBackend;
use crate::run::serve_worker;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::Result;
use tokio::net::TcpListener;
use tokio::sync::watch;

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
        for i in 0..count {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            ports.push(listener.local_addr()?.port());
            let backend = backend.clone();
            let mut shutdown_rx = shutdown_rx.clone();
            let signal = async move {
                let _ = shutdown_rx.changed().await;
            };
            tasks.push(SpawnedTask::spawn(async move {
                if let Err(err) = serve_worker(backend, listener, signal).await {
                    eprintln!("Local worker {i} failed: {err}");
                }
            }));
        }
        Ok(Self {
            tasks,
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
