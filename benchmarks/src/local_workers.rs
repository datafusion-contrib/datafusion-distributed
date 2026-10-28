use crate::backend::BenchmarkBackend;
use crate::run::serve_worker;
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::Result;
use tokio::net::TcpListener;

/// Localhost workers running in the current process.
///
/// Workers are spawned on the benchmark runner's own Tokio runtime rather than getting one
/// runtime each, so the process has a single pool of scheduler threads and a single blocking
/// pool, and profilers attached to it observe the coordinator and the workers on the same few
/// threads.
pub(crate) struct LocalWorkers {
    /// Aborted when dropped, which stops the workers.
    _tasks: Vec<SpawnedTask<()>>,
    pub(crate) ports: Vec<u16>,
}

impl LocalWorkers {
    /// Starts `count` workers on the current Tokio runtime, listening on OS-assigned localhost
    /// ports.
    pub(crate) async fn start<B: BenchmarkBackend + Clone>(
        count: usize,
        backend: &B,
    ) -> Result<Self> {
        let mut tasks = Vec::with_capacity(count);
        let mut ports = Vec::with_capacity(count);
        for i in 0..count {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            ports.push(listener.local_addr()?.port());
            let backend = backend.clone();
            tasks.push(SpawnedTask::spawn(async move {
                if let Err(err) = serve_worker(backend, listener).await {
                    eprintln!("Local worker {i} failed: {err}");
                }
            }));
        }
        Ok(Self {
            _tasks: tasks,
            ports,
        })
    }
}
