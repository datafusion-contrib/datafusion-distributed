pub mod in_memory_channel_resolver;
pub mod insta;
pub mod localhost;
pub mod metrics;
pub mod mock_exec;
pub mod parquet;
pub mod plans;
pub mod property_based;
pub mod routing;
pub mod session_context;
pub mod test_work_unit_feed;
pub mod work_unit_file_scan;

use std::path::Path;

/// Root directory of the repository, which holds the shared `testdata/` directory.
pub fn workspace_root() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

/// Absolute path of `relative` inside the repository's shared `testdata/` directory.
pub fn testdata_path(relative: &str) -> String {
    let path = workspace_root().join("testdata").join(relative);
    let path = path.canonicalize().unwrap_or(path);
    path.to_string_lossy().into_owned()
}
