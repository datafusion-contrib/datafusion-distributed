use crate::PyDistributedConfig;
use crate::localhost_channel_resolver::PyLocalhostChannelResolver;
use datafusion::common::Result;
use datafusion::execution::Session;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use datafusion_distributed::{
    ChannelResolver, DistributedCodec, DistributedConfig, DistributedExt, WorkerResolver,
    distribute_physical_plan,
};
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_ffi::query_planner::FFI_QueryPlanner;
use datafusion_proto::physical_plan::{ComposedPhysicalExtensionCodec, PhysicalExtensionCodec};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use std::ptr::NonNull;
use std::sync::{Arc, OnceLock};
use tokio::runtime::{Handle, Runtime};

#[pyclass(
    name = "DistributedQueryPlanner",
    module = "datafusion_distributed._internal",
    skip_from_py_object
)]
pub(crate) struct PyDistributedQueryPlanner {
    resolver: PyLocalhostChannelResolver,
    logical_codec: FFI_LogicalExtensionCodec,
    physical_codec: FFI_PhysicalExtensionCodec,
    task_ctx_provider: FFI_TaskContextProvider,
    _session_ctx: Py<PyAny>,
    inner: Option<FFI_QueryPlanner>,
    distributed_config: DistributedConfig,
}

#[pymethods]
impl PyDistributedQueryPlanner {
    #[new]
    #[pyo3(signature = (resolver, session_ctx, inner=None, config=None))]
    fn new(
        resolver: PyRef<'_, PyLocalhostChannelResolver>,
        session_ctx: Bound<'_, PyAny>,
        inner: Option<Bound<'_, PyAny>>,
        config: Option<PyRef<'_, PyDistributedConfig>>,
    ) -> PyResult<Self> {
        let logical_codec = ffi_logical_codec_from_python(session_ctx.clone())?;
        let physical_codec = ffi_physical_codec_from_python(session_ctx.clone())?;
        let task_ctx_provider = ffi_task_ctx_provider_from_python(session_ctx.clone())?;
        let session_ctx = session_ctx.unbind();
        let inner = inner
            .as_ref()
            .map(ffi_query_planner_from_python)
            .transpose()?;
        Ok(Self {
            resolver: resolver.clone(),
            logical_codec,
            physical_codec,
            task_ctx_provider,
            _session_ctx: session_ctx,
            inner,
            distributed_config: config
                .as_ref()
                .map(|config| config.inner.clone())
                .unwrap_or_default(),
        })
    }

    fn __datafusion_query_planner__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let user_codec: Arc<dyn PhysicalExtensionCodec> = (&self.physical_codec).into();
        let physical_codec: Arc<dyn PhysicalExtensionCodec + Send> =
            Arc::new(ComposedPhysicalExtensionCodec::new(vec![
                Arc::clone(&user_codec),
                Arc::new(DistributedCodec {}),
            ]));
        let physical_codec = FFI_PhysicalExtensionCodec::new(
            physical_codec,
            Some(tokio_runtime_handle()),
            self.task_ctx_provider.clone(),
        );
        let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(DistributedFfiQueryPlanner {
            resolver: self.resolver.clone(),
            user_codec,
            _session_ctx: self._session_ctx.clone_ref(py),
            inner: self.inner.clone(),
            distributed_config: self.distributed_config.clone(),
        });
        let ffi = FFI_QueryPlanner::new_with_ffi_codecs(
            planner,
            self.logical_codec.clone(),
            physical_codec,
        );

        PyCapsule::new(py, ffi, Some(cr"datafusion_query_planner_v1".into()))
    }
}

struct DistributedFfiQueryPlanner {
    resolver: PyLocalhostChannelResolver,
    user_codec: Arc<dyn PhysicalExtensionCodec>,
    _session_ctx: Py<PyAny>,
    inner: Option<FFI_QueryPlanner>,
    distributed_config: DistributedConfig,
}

impl std::fmt::Debug for DistributedFfiQueryPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedFfiQueryPlanner")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl QueryPlanner for DistributedFfiQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let physical_plan = match &self.inner {
            Some(inner) => {
                inner
                    .create_physical_plan_with_session_runtime(
                        logical_plan,
                        session,
                        Some(tokio_runtime_handle()),
                    )
                    .await?
            }
            None => {
                let planner = DefaultPhysicalPlanner::default();
                planner.create_physical_plan(logical_plan, session).await?
            }
        };

        let mut session_config = session.config().clone();
        session_config.set_distributed_user_codec_arc(Arc::clone(&self.user_codec));
        let distributed_config =
            DistributedConfig::from_config_options_owned(session_config.options())
                .unwrap_or_else(|_| self.distributed_config.clone());
        session_config
            .options_mut()
            .extensions
            .insert(distributed_config);

        let resolver = self.resolver.resolver();
        let channel_resolver: Arc<dyn ChannelResolver + Send + Sync> = resolver.clone();
        let worker_resolver: Arc<dyn WorkerResolver + Send + Sync> = resolver;
        session_config.set_distributed_channel_resolver(channel_resolver);
        session_config.set_distributed_worker_resolver(worker_resolver);

        distribute_physical_plan(physical_plan, session_config.options()).await
    }
}

#[pyfunction]
#[pyo3(signature = (session_ctx, resolver, config=None))]
pub(crate) fn with_distributed_query_planner(
    py: Python<'_>,
    session_ctx: Bound<'_, PyAny>,
    resolver: PyRef<'_, PyLocalhostChannelResolver>,
    config: Option<PyRef<'_, PyDistributedConfig>>,
) -> PyResult<Py<PyAny>> {
    let inner = session_ctx
        .getattr("__datafusion_query_planner__")?
        .call0()?;
    let planner =
        PyDistributedQueryPlanner::new(resolver, session_ctx.clone(), Some(inner), config)?;
    let planner = Py::new(py, planner)?;
    Ok(session_ctx
        .call_method1("with_query_planner", (planner,))?
        .unbind())
}

fn tokio_runtime_handle() -> Handle {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| Runtime::new().expect("tokio runtime for datafusion-distributed Python"))
        .handle()
        .clone()
}

fn ffi_logical_codec_from_python(obj: Bound<'_, PyAny>) -> PyResult<FFI_LogicalExtensionCodec> {
    let capsule = obj
        .getattr("__datafusion_logical_extension_codec__")?
        .call0()?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_LogicalExtensionCodec> = capsule
        .pointer_checked(Some(c"datafusion_logical_extension_codec"))?
        .cast();
    Ok(unsafe { data.as_ref().clone() })
}

fn ffi_task_ctx_provider_from_python(obj: Bound<'_, PyAny>) -> PyResult<FFI_TaskContextProvider> {
    let capsule = obj
        .getattr("__datafusion_task_context_provider__")?
        .call0()?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_TaskContextProvider> = capsule
        .pointer_checked(Some(c"datafusion_task_context_provider"))?
        .cast();
    Ok(unsafe { data.as_ref().clone() })
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

fn ffi_query_planner_from_python(obj: &Bound<'_, PyAny>) -> PyResult<FFI_QueryPlanner> {
    let capsule = if obj.hasattr("__datafusion_query_planner__")? {
        obj.getattr("__datafusion_query_planner__")?.call0()?
    } else {
        obj.clone()
    };
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_QueryPlanner> = capsule
        .pointer_checked(Some(c"datafusion_query_planner_v1"))?
        .cast();
    Ok(unsafe { data.as_ref().clone() })
}
