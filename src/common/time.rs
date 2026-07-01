use num_traits::AsPrimitive;
use std::time::{SystemTime, UNIX_EPOCH};

/// Nanoseconds elapsed since UNIX epoch.
pub(crate) fn now_ns<T>() -> T
where
    T: 'static + Copy,
    u128: AsPrimitive<T>,
{
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos().as_())
        .expect("SystemTime before UNIX EPOCH!")
}
