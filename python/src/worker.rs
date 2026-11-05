use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;

use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion_distributed::{DistributedExt, Worker as NativeWorker, WorkerQueryContext};
use datafusion_python::codec::PythonPhysicalCodec;
use datafusion_python_util::{get_tokio_runtime, wait_for_future};
use log::info;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

struct RunningWorker {
    url: String,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), tonic::transport::Error>>,
}

#[pyclass(name = "Worker", module = "datafusion_distributed._internal")]
pub(crate) struct PyWorker {
    worker: NativeWorker,
    running: Mutex<Option<RunningWorker>>,
}

#[pymethods]
impl PyWorker {
    #[new]
    fn new() -> Self {
        Self {
            worker: NativeWorker::from_session_builder(build_python_worker_session),
            running: Mutex::new(None),
        }
    }

    fn run(&self, py: Python<'_>, host: &str, port: u16) -> PyResult<()> {
        if self.is_running()? {
            return Err(PyRuntimeError::new_err("worker is already running"));
        }

        let listener = wait_for_future(py, TcpListener::bind(worker_address(host, port)?))?
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let url = format!("http://{address}");
        let worker = self.worker.clone();

        info!(target: "datafusion_distributed.worker", "Worker listening on {url}");
        let result = wait_for_future(py, async move {
            Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
        });
        info!(target: "datafusion_distributed.worker", "Worker stopped on {url}");
        result?.map_err(|err| PyRuntimeError::new_err(err.to_string()))
    }

    fn start(&self, py: Python<'_>, host: &str, port: u16) -> PyResult<String> {
        if self.is_running()? {
            return Err(PyRuntimeError::new_err("worker is already running"));
        }

        let listener = wait_for_future(py, TcpListener::bind(worker_address(host, port)?))?
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        let address = listener
            .local_addr()
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;

        let mut running = self
            .running
            .lock()
            .map_err(|_| PyRuntimeError::new_err("worker state lock is poisoned"))?;
        if running.is_some() {
            return Err(PyRuntimeError::new_err("worker is already running"));
        }

        let worker = self.worker.clone();
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let url = format!("http://{address}");
        let task = get_tokio_runtime().spawn(async move {
            Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                })
                .await
        });
        *running = Some(RunningWorker {
            url: url.clone(),
            shutdown,
            task,
        });
        info!(target: "datafusion_distributed.worker", "Worker listening on {url}");

        Ok(url)
    }

    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        let running = self
            .running
            .lock()
            .map_err(|_| PyRuntimeError::new_err("worker state lock is poisoned"))?
            .take();
        let Some(running) = running else {
            return Ok(());
        };

        let _ = running.shutdown.send(());
        let result = wait_for_future(py, running.task)?
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?
            .map_err(|err| PyRuntimeError::new_err(err.to_string()));
        info!(
            target: "datafusion_distributed.worker",
            "Worker stopped on {}",
            running.url
        );
        result
    }
}

impl PyWorker {
    fn is_running(&self) -> PyResult<bool> {
        self.running
            .lock()
            .map(|running| running.is_some())
            .map_err(|_| PyRuntimeError::new_err("worker state lock is poisoned"))
    }
}

impl Drop for PyWorker {
    fn drop(&mut self) {
        if let Ok(running) = self.running.get_mut()
            && let Some(running) = running.take()
        {
            let _ = running.shutdown.send(());
        }
    }
}

async fn build_python_worker_session(
    ctx: WorkerQueryContext,
) -> Result<SessionState, DataFusionError> {
    Ok(ctx
        .builder
        .with_distributed_user_codec(PythonPhysicalCodec::default())
        .build())
}

fn worker_address(host: &str, port: u16) -> PyResult<SocketAddr> {
    let host = host
        .parse::<IpAddr>()
        .map_err(|err| PyValueError::new_err(err.to_string()))?;
    Ok(SocketAddr::new(host, port))
}
