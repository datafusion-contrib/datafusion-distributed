use thiserror::Error;

#[derive(Debug, Error)]
pub enum DFRayError {
    #[error("Internal Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Internal DataFusion error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error("Failed to communicate with worker: {0}")]
    // the name of the worker
    // improve this as we swallow the error here
    WorkerCommunicationError(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = DFRayError> = std::result::Result<T, E>;
