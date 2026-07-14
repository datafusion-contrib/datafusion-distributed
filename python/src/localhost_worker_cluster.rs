use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionState;
use datafusion_distributed::{
    DistributedConfig, DistributedExt, Worker, WorkerQueryContext, WorkerSessionBuilder,
};
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ptr::NonNull;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::localhost_channel_resolver::PyLocalhostChannelResolver;

#[derive(Clone)]
struct FfiWorkerSessionBuilder {
    physical_codec: Arc<dyn PhysicalExtensionCodec>,
}

#[async_trait]
impl WorkerSessionBuilder for FfiWorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_distributed_option_extension(DistributedConfig::default())
            .with_distributed_user_codec_arc(Arc::clone(&self.physical_codec))
            .build())
    }
}

/// A temporary localhost worker cluster for Python integration tests.
#[pyclass(
    name = "LocalhostWorkerCluster",
    module = "datafusion_distributed._internal",
    skip_from_py_object
)]
pub(crate) struct PyLocalhostWorkerCluster {
    resolver: PyLocalhostChannelResolver,
    workers: JoinSet<()>,
}

#[pymethods]
impl PyLocalhostWorkerCluster {
    #[new]
    #[pyo3(signature = (session_ctx, worker_count=2))]
    fn new(session_ctx: Bound<'_, PyAny>, worker_count: usize) -> PyResult<Self> {
        if worker_count == 0 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "worker_count must be greater than zero",
            ));
        }

        let physical_codec = ffi_physical_codec_from_python(session_ctx)?;
        let physical_codec: Arc<dyn PhysicalExtensionCodec> = (&physical_codec).into();
        let (ports, workers) = worker_runtime()
            .block_on(start_workers(worker_count, physical_codec))
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;

        Ok(Self {
            resolver: PyLocalhostChannelResolver::from_ports(ports)?,
            workers,
        })
    }

    fn resolver(&self) -> PyLocalhostChannelResolver {
        self.resolver.clone()
    }

    fn urls(&self) -> Vec<String> {
        self.resolver.url_strings()
    }
}

impl Drop for PyLocalhostWorkerCluster {
    fn drop(&mut self) {
        self.workers.abort_all();
    }
}

async fn start_workers(
    worker_count: usize,
    physical_codec: Arc<dyn PhysicalExtensionCodec>,
) -> Result<(Vec<u16>, JoinSet<()>), std::io::Error> {
    let mut ports = Vec::with_capacity(worker_count);
    let mut workers = JoinSet::new();

    for _ in 0..worker_count {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        ports.push(listener.local_addr()?.port());
        let worker = Worker::from_session_builder(FfiWorkerSessionBuilder {
            physical_codec: Arc::clone(&physical_codec),
        });
        workers.spawn(async move {
            Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("localhost test worker failed");
        });
    }

    Ok((ports, workers))
}

fn worker_runtime() -> &'static Runtime {
    use std::sync::OnceLock;

    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("tokio runtime for localhost test workers"))
}

fn ffi_physical_codec_from_python(obj: Bound<'_, PyAny>) -> PyResult<FFI_PhysicalExtensionCodec> {
    let capsule = obj
        .getattr("__datafusion_physical_extension_codec__")?
        .call0()?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_PhysicalExtensionCodec> = capsule
        .pointer_checked(Some(c"datafusion_physical_extension_codec"))?
        .cast();
    Ok(unsafe { data.as_ref().clone() })
}
