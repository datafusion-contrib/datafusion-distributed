/// we are using our own logging macros here so that
/// we can add functionality to them like logging task local variables
/// or routing to additional or alternative destinations if required.
macro_rules! make_log_macro (
    (($d:tt) $level:ident) => (
        #[allow(unused)]
        macro_rules! $level(
            ( $d($x:expr),* ) => (
                // just defer to log crate for now
                log::$level!($d($x),*)
            )
        );
        pub(crate) use $level;

    );
    ($($t:tt)+) => (
        make_log_macro!(($) $($t)+);
    );
);

make_log_macro!(error);
make_log_macro!(trace);
make_log_macro!(debug);
make_log_macro!(info);
