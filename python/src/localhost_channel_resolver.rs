use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion_distributed::{
    BoxCloneSyncChannel, ChannelResolver, DefaultChannelResolver, WorkerResolver,
    WorkerServiceClient,
};
use pyo3::prelude::*;
use std::sync::Arc;
use url::Url;

#[pyclass(
    name = "LocalhostChannelResolver",
    module = "datafusion_distributed._internal",
    skip_from_py_object
)]
#[derive(Clone)]
pub(crate) struct PyLocalhostChannelResolver {
    inner: Arc<LocalhostChannelResolver>,
}

#[pymethods]
impl PyLocalhostChannelResolver {
    #[new]
    fn new(ports: Vec<u16>) -> PyResult<Self> {
        Self::from_ports(ports)
    }

    fn urls(&self) -> Vec<String> {
        self.url_strings()
    }
}

impl PyLocalhostChannelResolver {
    pub(crate) fn from_ports(ports: Vec<u16>) -> PyResult<Self> {
        let urls = ports
            .into_iter()
            .map(|port| {
                Url::parse(&format!("http://127.0.0.1:{port}"))
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
            })
            .collect::<PyResult<Vec<_>>>()?;

        Ok(Self {
            inner: Arc::new(LocalhostChannelResolver {
                urls,
                channel_resolver: DefaultChannelResolver::default(),
            }),
        })
    }

    pub(crate) fn url_strings(&self) -> Vec<String> {
        self.inner.urls.iter().map(ToString::to_string).collect()
    }

    #[allow(dead_code)]
    pub(crate) fn resolver(&self) -> Arc<LocalhostChannelResolver> {
        Arc::clone(&self.inner)
    }
}

pub(crate) struct LocalhostChannelResolver {
    urls: Vec<Url>,
    channel_resolver: DefaultChannelResolver,
}

impl WorkerResolver for LocalhostChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self.urls.clone())
    }
}

#[async_trait]
impl ChannelResolver for LocalhostChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        url: &Url,
    ) -> Result<WorkerServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        self.channel_resolver.get_worker_client_for_url(url).await
    }
}
