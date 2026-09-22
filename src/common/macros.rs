#[macro_export]
macro_rules! ok_or_some_err {
    ($expr: expr) => {
        match $expr {
            Ok(v) => v,
            Err(err) => return Some(Err(err)),
        }
    };
}
