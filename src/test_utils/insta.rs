use std::env;

pub use insta;

#[macro_export]
macro_rules! assert_snapshot {
    ($($arg:tt)*) => {
        $crate::test_utils::insta::settings().bind(|| {
            $crate::test_utils::insta::insta::assert_snapshot!($($arg)*);
        })
    };
}

pub fn settings() -> insta::Settings {
    // Safety: this is only used in tests, it may panic if used in parallel with other tests.
    unsafe { env::set_var("INSTA_WORKSPACE_ROOT", env!("CARGO_MANIFEST_DIR")) };
    let mut settings = insta::Settings::clone_current();
    let cwd = env::current_dir().unwrap();
    let cwd = cwd.to_str().unwrap();
    settings.add_filter(cwd.trim_start_matches("/"), "");
    settings.add_filter(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        "UUID",
    );

    settings
}
