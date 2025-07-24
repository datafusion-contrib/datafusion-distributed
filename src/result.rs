use arrow_flight::error::FlightError;
use thiserror::Error;

use crate::vocab::Host;

/// Error types for distributed DataFusion operations
///
/// This enum encompasses all possible errors that can occur during distributed
/// query execution, from Arrow/DataFusion internal errors to distributed-specific
/// issues like worker communication failures.
#[derive(Debug, Error)]
pub enum DDError {
    /// Arrow library errors (schema mismatches, memory allocation, data conversion)
    #[error("Internal Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    /// DataFusion query engine errors (planning, optimization, execution failures)
    #[error("Internal DataFusion error: {0}")]
    DataFusionError(#[from] datafusion::error::DataFusionError),

    /// Arrow Flight protocol errors (network communication, serialization)
    #[error("Arrow Flight error: {0}")]
    FlightError(#[from] FlightError),

    /// Worker communication failures during distributed execution
    #[error("Failed to communicate with worker: {0}")]
    // TODO: Improve error propagation - currently swallows underlying error details
    WorkerCommunicationError(Host),

    /// Catch-all for other errors using anyhow for flexibility
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Standard Result type for distributed DataFusion operations
///
/// This type alias uses DDError as the default error type, providing consistent
/// error handling across the distributed DataFusion codebase.
pub type Result<T, E = DDError> = std::result::Result<T, E>;
