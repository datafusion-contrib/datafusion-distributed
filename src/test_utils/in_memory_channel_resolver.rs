use crate::WorkerResolver;
use datafusion::common::DataFusionError;
use url::Url;

const DUMMY_URL_PREFIX: &str = "http://url-";

pub struct InMemoryWorkerResolver {
    n_workers: usize,
}

impl InMemoryWorkerResolver {
    pub fn new(n_workers: usize) -> Self {
        Self { n_workers }
    }
}

impl WorkerResolver for InMemoryWorkerResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        (0..self.n_workers)
            .map(|i| Url::parse(&format!("{}{}", DUMMY_URL_PREFIX, i)))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| DataFusionError::External(Box::new(err)))
    }
}

// The gRPC-specific channel resolver implementations have been moved to crate::protocol::grpc::test_utils.
