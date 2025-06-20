use arrow_flight::error::FlightError;
use thiserror::Error;

use crate::vocab::Host;

#[derive(Debug, Error)]
pub enum DDError {
    #[error("Internal Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Internal DataFusion error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    #[error("Arrow Flight error: {0}")]
    FlightError(#[from] FlightError),

    #[error("Failed to communicate with worker: {0}")]
    // the name of the worker
    // improve this as we swallow the error here
    WorkerCommunicationError(Host),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T, E = DDError> = std::result::Result<T, E>;
