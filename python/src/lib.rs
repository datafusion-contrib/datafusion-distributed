//! Registration for the `datafusion_distributed._internal` native module.
//!
//! This extension links datafusion-python in order to own its distributed
//! [`PySessionContext`](datafusion_python::context::PySessionContext). The installed
//! `datafusion` wheel loads a different native extension, so even identically named
//! PyO3 classes from the two modules have different Python type identities. Opaque
//! values without a serialization or FFI protocol must be registered here and
//! constructed through this package's Python wrappers.

mod context;
mod expression;
mod user_defined;
mod worker;
mod worker_resolver;

use pyo3::prelude::*;

// When adding new symbols exposed to python here, remember to update the _internal.pyi definitions.
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    // These opaque configuration values are consumed directly by the local
    // PySessionContext and have no cross-extension import/export protocol.
    m.add_class::<datafusion_python::context::PySessionConfig>()?;
    m.add_class::<datafusion_python::context::PyRuntimeEnvBuilder>()?;

    // StorageContexts is a native enum over these exact PyO3 classes. An object
    // from datafusion.object_store has a different class identity, and object
    // stores currently have neither a protobuf nor an FFI bridge.
    let object_store = PyModule::new(m.py(), "object_store")?;
    object_store.add_class::<datafusion_python::store::PyAmazonS3Context>()?;
    object_store.add_class::<datafusion_python::store::PyGoogleCloudContext>()?;
    object_store.add_class::<datafusion_python::store::PyHttpContext>()?;
    object_store.add_class::<datafusion_python::store::PyLocalFileSystemContext>()?;
    object_store.add_class::<datafusion_python::store::PyMicrosoftAzureContext>()?;
    m.add_submodule(&object_store)?;

    // Exposed from the context.rs module
    m.add_function(wrap_pyfunction!(context::create_distributed_session, m)?)?;
    m.add_function(wrap_pyfunction!(context::rebind_distributed_planner, m)?)?;

    // Exposed from the expression.rs module.
    m.add_function(wrap_pyfunction!(expression::deserialize_expression, m)?)?;
    m.add_function(wrap_pyfunction!(expression::serialize_expression, m)?)?;

    // Exposed from the user_defined.rs module.
    m.add_function(wrap_pyfunction!(user_defined::register_scalar_udf, m)?)?;
    m.add_function(wrap_pyfunction!(user_defined::register_aggregate_udf, m)?)?;
    m.add_function(wrap_pyfunction!(user_defined::register_window_udf, m)?)?;

    // Exposed from the worker.rs module.
    m.add_class::<worker::PyWorker>()?;

    Ok(())
}
