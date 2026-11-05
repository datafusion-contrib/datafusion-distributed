use datafusion::common::{Result, exec_datafusion_err};
use datafusion_distributed::WorkerResolver;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use url::Url;

pub(crate) struct PythonWorkerResolver {
    resolver: Py<PyAny>,
}

impl PythonWorkerResolver {
    pub(crate) fn try_new(py: Python<'_>, resolver: Py<PyAny>) -> PyResult<Self> {
        let get_urls = resolver
            .bind(py)
            .getattr("get_urls")
            .map_err(|_| PyTypeError::new_err("worker_resolver must define a get_urls() method"))?;
        if !get_urls.is_callable() {
            return Err(PyTypeError::new_err(
                "worker_resolver.get_urls must be callable",
            ));
        }

        Ok(Self { resolver })
    }
}

impl WorkerResolver for PythonWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>> {
        let urls = Python::attach(|py| {
            self.resolver
                .bind(py)
                .call_method0("get_urls")
                .and_then(|urls| urls.extract::<Vec<String>>())
                .map_err(|err| {
                    exec_datafusion_err!("Python WorkerResolver.get_urls() failed: {err}")
                })
        })?;

        urls.into_iter()
            .map(|url| {
                Url::parse(&url).map_err(|err| {
                    exec_datafusion_err!(
                        "Python WorkerResolver returned invalid URL '{url}': {err}"
                    )
                })
            })
            .collect()
    }
}
