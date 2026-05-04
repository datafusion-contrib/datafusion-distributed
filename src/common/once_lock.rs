use datafusion::error::DataFusionError;
use std::sync::{Arc, OnceLock};

/// A [OnceLock] that holds a clonable result.
pub(crate) type OnceLockResult<T> = OnceLock<Result<T, Arc<DataFusionError>>>;
