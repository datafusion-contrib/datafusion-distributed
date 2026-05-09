use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the number of nanoseconds since Unix epoch.
pub(crate) fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}
