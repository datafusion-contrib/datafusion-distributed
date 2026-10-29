use std::sync::Arc;

use datafusion::execution::SessionStateBuilder;
use datafusion::execution::TaskContextProvider;
use datafusion::execution::context::QueryPlanner;
use datafusion::prelude::SessionConfig;
use datafusion_distributed::{
    DistributedCodec, DistributedExt, DistributedQueryPlanner, inject_distributed_extensions,
};
use datafusion_ffi::proto::physical_extension_codec::{
    FFI_PhysicalExtensionCodec, ForeignPhysicalExtensionCodec,
};
use datafusion_ffi::query_planner::{FFI_QueryPlanner, ForeignQueryPlanner};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_python::context::{PyRuntimeEnvBuilder, PySessionConfig, PySessionContext};
use datafusion_python_util::{
    create_physical_extension_capsule, ffi_logical_codec_from_pycapsule,
    ffi_physical_codec_from_pycapsule, get_tokio_runtime,
};
use pyo3::prelude::*;

use crate::worker_resolver::PythonWorkerResolver;

const DISTRIBUTED_CODEC_ID: &str = "datafusion_distributed.physical.v1";

/// Wraps `planner` in an FFI planner that always crosses the protobuf boundary.
///
/// The wrapper encodes the logical-plan input before invoking `planner`, then encodes and decodes
/// its physical-plan result with the extension codecs currently installed on `context`. This
/// reconstructs DataFusion's built-in physical nodes as concrete types in this dynamic library,
/// allowing [`DistributedQueryPlanner`] to inspect and rewrite them rather than receiving opaque
/// foreign execution-plan handles.
fn force_proto_roundtrip(
    context: &Bound<'_, PySessionContext>,
    planner: Arc<dyn QueryPlanner + Send + Sync>,
) -> PyResult<Arc<dyn QueryPlanner + Send + Sync>> {
    let logical = context.call_method0("__datafusion_logical_extension_codec__")?;
    let physical = context.call_method0("__datafusion_physical_extension_codec__")?;
    let logical = ffi_logical_codec_from_pycapsule(logical, None)?;
    let physical = ffi_physical_codec_from_pycapsule(physical, None)?;
    let ffi = FFI_QueryPlanner::new_with_ffi_codecs(planner, logical, physical);

    Ok(Arc::new(ForeignQueryPlanner(ffi)))
}

/// Installs the distributed planner outside the session's existing planner.
///
/// Rebinding first unwraps an already-installed [`DistributedQueryPlanner`] so repeated codec
/// updates replace the adapter instead of nesting distributed planners. The previous planner is
/// then forced through [`force_proto_roundtrip`] before the distributed planner sees its output.
fn install_distributed_planner(context: &Bound<'_, PySessionContext>) -> PyResult<()> {
    let current = Arc::clone(context.borrow().ctx.state_ref().read().query_planner());
    let previous = (&current as &dyn std::any::Any)
        .downcast_ref::<DistributedQueryPlanner>()
        .and_then(DistributedQueryPlanner::previous)
        .unwrap_or(current);
    let previous_plus_ffi_roundtrip = force_proto_roundtrip(context, previous)?;
    let previous_plus_ffi_roundtrip_plus_distributed = Arc::new(DistributedQueryPlanner::new(
        Some(previous_plus_ffi_roundtrip),
    ));

    let state_ref = context.borrow().ctx.state_ref();
    let mut state = state_ref.write();
    let session_id = state.session_id().to_string();
    *state = SessionStateBuilder::new_from_existing(state.clone())
        .with_session_id(session_id)
        .with_query_planner(previous_plus_ffi_roundtrip_plus_distributed)
        .build();
    Ok(())
}

/// Creates a Python session configured for distributed planning and execution.
///
/// The supplied configuration remains session state: this function injects the distributed
/// options and Python worker resolver before constructing the context. It then installs the
/// distributed physical codec through FFI, makes the resulting codec chain available through the
/// session's task contexts, and places [`DistributedQueryPlanner`] outside DataFusion's original
/// planner.
#[pyfunction]
#[pyo3(signature = (worker_resolver, config=None, runtime=None))]
pub(crate) fn create_distributed_session(
    py: Python<'_>,
    worker_resolver: Py<PyAny>,
    config: Option<PySessionConfig>,
    runtime: Option<PyRuntimeEnvBuilder>,
) -> PyResult<Py<PySessionContext>> {
    let worker_resolver = PythonWorkerResolver::try_new(py, worker_resolver)?;

    let mut config = config
        .map(|config| config.config)
        .unwrap_or_else(|| SessionConfig::default().with_information_schema(true));
    inject_distributed_extensions(&mut config);
    config.set_distributed_worker_resolver(worker_resolver);

    let context = PySessionContext::new(Some(PySessionConfig::from(config)), runtime)?;
    let context = Py::new(py, context)?;

    let provider = {
        let context = context.borrow(py);
        Arc::clone(&context.ctx) as Arc<dyn TaskContextProvider>
    };
    let codec: Arc<dyn PhysicalExtensionCodec + Send> = Arc::new(DistributedCodec);
    let codec = FFI_PhysicalExtensionCodec::new(
        codec,
        Some(get_tokio_runtime().handle().clone()),
        &provider,
    );
    let capsule = create_physical_extension_capsule(py, &codec)?;
    let context = PySessionContext::with_physical_extension_codec(
        context.bind(py),
        capsule.into_any(),
        Some(DISTRIBUTED_CODEC_ID.to_string()),
    )?;
    let context = Py::new(py, context)?;
    install_task_physical_codec(context.bind(py))?;
    install_distributed_planner(context.bind(py))?;
    Ok(context)
}

/// Makes the session's physical extension codecs available to physical-plan decoding.
///
/// The composed codec is imported through FFI and stored in the session configuration as the
/// distributed user codec. DataFusion copies that configuration into each `TaskContext`, allowing
/// [`DistributedCodec`] to recover the user codec contextually while decoding a plan. Its lifetime
/// is therefore tied to the session rather than to an execution plan. Rebuilding the state
/// preserves the session id and all other existing session state.
fn install_task_physical_codec(context: &Bound<'_, PySessionContext>) -> PyResult<()> {
    let physical = context.call_method0("__datafusion_physical_extension_codec__")?;
    let physical = ffi_physical_codec_from_pycapsule(physical, None)?;

    let state_ref = context.borrow().ctx.state_ref();
    let mut state = state_ref.write();
    let mut config = state.config().clone();
    config.set_distributed_user_codec(ForeignPhysicalExtensionCodec(physical));
    let session_id = state.session_id().to_string();
    *state = SessionStateBuilder::new_from_existing(state.clone())
        .with_config(config)
        .with_session_id(session_id)
        .build();
    Ok(())
}

/// Rebuilds the distributed planner against the context's current codec chains.
///
/// DataFusion's `with_*_extension_codec` methods return a context whose codecs differ from the
/// original handle, while [`ForeignQueryPlanner`] captures its FFI codecs when it is constructed.
/// The Python wrapper calls this function after deriving such a context so subsequent planning
/// uses the newly installed codecs without stacking another distributed planner.
#[pyfunction]
pub(crate) fn rebind_distributed_planner(context: &Bound<'_, PySessionContext>) -> PyResult<()> {
    install_distributed_planner(context)
}
