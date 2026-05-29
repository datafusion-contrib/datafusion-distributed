use std::time::{SystemTime, UNIX_EPOCH};

/// Nanoseconds elapsed since UNIX epoch.
pub(crate) fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .expect("SystemTime before UNIX EPOCH!")
}
