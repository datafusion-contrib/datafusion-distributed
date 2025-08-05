use datafusion::common::utils::get_available_parallelism;
use std::env;

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
    let cpus = get_available_parallelism();
    settings.add_filter(&format!(", {cpus}\\)"), ", CPUs)");
    settings.add_filter(&format!("\\({cpus}\\)"), "(CPUs)");
    settings.add_filter(&format!("input_partitions={cpus}"), "input_partitions=CPUs");
    settings.add_filter(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        "UUID",
    );

    settings
}
