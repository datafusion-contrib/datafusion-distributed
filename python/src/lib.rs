mod localhost_channel_resolver;
mod localhost_worker_cluster;
mod query_planner;

use datafusion_distributed::DistributedConfig as RustDistributedConfig;
use datafusion_ffi::config::extension_options::FFI_ExtensionOptions;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[pyclass(
    name = "DistributedConfig",
    module = "datafusion_distributed._internal",
    skip_from_py_object
)]
#[derive(Debug, Clone, Default)]
pub(crate) struct PyDistributedConfig {
    pub(crate) inner: RustDistributedConfig,
}

#[pymethods]
impl PyDistributedConfig {
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn with_file_scan_config_bytes_per_partition(&mut self, value: usize) -> PyResult<()> {
        self.inner.file_scan_config_bytes_per_partition = value;
        Ok(())
    }

    fn with_cardinality_task_count_factor(&mut self, value: f64) -> PyResult<()> {
        self.inner.cardinality_task_count_factor = value;
        Ok(())
    }

    fn with_children_isolator_unions(&mut self, value: bool) -> PyResult<()> {
        self.inner.children_isolator_unions = value;
        Ok(())
    }

    fn with_collect_metrics(&mut self, value: bool) -> PyResult<()> {
        self.inner.collect_metrics = value;
        Ok(())
    }

    fn with_broadcast_joins(&mut self, value: bool) -> PyResult<()> {
        self.inner.broadcast_joins = value;
        Ok(())
    }

    fn with_compression(&mut self, value: String) -> PyResult<()> {
        self.inner.compression = value;
        Ok(())
    }

    fn with_shuffle_batch_size(&mut self, value: usize) -> PyResult<()> {
        self.inner.shuffle_batch_size = value;
        Ok(())
    }

    fn with_max_tasks_per_stage(&mut self, value: usize) -> PyResult<()> {
        self.inner.max_tasks_per_stage = value;
        Ok(())
    }

    fn with_partial_reduce(&mut self, value: bool) -> PyResult<()> {
        self.inner.partial_reduce = value;
        Ok(())
    }

    fn with_worker_connection_buffer_budget_bytes(&mut self, value: usize) -> PyResult<()> {
        self.inner.worker_connection_buffer_budget_bytes = value;
        Ok(())
    }

    fn __datafusion_extension_options__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let mut ffi = FFI_ExtensionOptions::default();
        ffi.add_config(&self.inner)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        PyCapsule::new(py, ffi, Some(cr"datafusion_extension_options".into()))
    }
}

/// Python extension module for datafusion-distributed.
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<PyDistributedConfig>()?;
    m.add_class::<localhost_channel_resolver::PyLocalhostChannelResolver>()?;
    m.add_class::<localhost_worker_cluster::PyLocalhostWorkerCluster>()?;
    m.add_class::<query_planner::PyDistributedQueryPlanner>()?;
    m.add_function(wrap_pyfunction!(
        query_planner::with_distributed_query_planner,
        m
    )?)?;
    Ok(())
}
