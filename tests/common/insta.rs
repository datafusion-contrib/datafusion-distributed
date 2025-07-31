use std::env;
use std::thread::available_parallelism;

#[macro_export]
macro_rules! assert_snapshot {
    ($($arg:tt)*) => {
        $crate::common::insta::settings().bind(|| {
            insta::assert_snapshot!($($arg)*);
        })
    };
}

pub fn settings() -> insta::Settings {
    env::set_var("INSTA_WORKSPACE_ROOT", env!("CARGO_MANIFEST_DIR"));
    let mut settings = insta::Settings::clone_current();
    let cwd = env::current_dir().unwrap();
    let cwd = cwd.to_str().unwrap();
    settings.add_filter(cwd.trim_start_matches("/"), "");
    let cpus = available_parallelism().unwrap();
    settings.add_filter(&format!(", {cpus}\\)"), ", CPUs)");
    settings.add_filter(&format!("\\({cpus}\\)"), "(CPUs)");
    settings.add_filter(&format!("={cpus}"), "=CPUs");

    settings
}
