use datafusion::error::DataFusionError;
use std::sync::{Arc, OnceLock};

/// A [OnceLock] that holds a cloneable result.
pub(crate) type OnceLockResult<T> = OnceLock<Result<T, Arc<DataFusionError>>>;
