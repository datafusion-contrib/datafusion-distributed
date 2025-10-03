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
    settings.add_filter(r"\d+\.\.\d+", "<int>..<int>");

    // Metric filters - only replace known metric names to avoid false positives
    settings.add_filter(r"output_rows=\d+", "output_rows=<metric>");
    settings.add_filter(
        r"elapsed_compute=[\d.]+[a-zA-Zµnms]+",
        "elapsed_compute=<metric>",
    );
    settings.add_filter(r"spill_count=\d+", "spill_count=<metric>");
    settings.add_filter(
        r"spilled_bytes=[\d.]+\s*[KMGTPE]?B?",
        "spilled_bytes=<metric>",
    );
    settings.add_filter(r"spilled_rows=\d+", "spilled_rows=<metric>");
    settings.add_filter(
        r"current_memory_usage=[\d.]+\s*[KMGTPE]?B?",
        "current_memory_usage=<metric>",
    );
    settings.add_filter(
        r"start_timestamp=[\d.]+[a-zA-Zµnms]*",
        "start_timestamp=<metric>",
    );
    settings.add_filter(
        r"end_timestamp=[\d.]+[a-zA-Zµnms]*",
        "end_timestamp=<metric>",
    );

    // Common custom metric patterns
    settings.add_filter(r"fetch_time=[\d.]+[a-zA-Zµnms]+", "fetch_time=<metric>");
    settings.add_filter(
        r"repartition_time=[\d.]+[a-zA-Zµnms]+",
        "repartition_time=<metric>",
    );
    settings.add_filter(r"send_time=[\d.]+[a-zA-Zµnms]+", "send_time=<metric>");
    settings.add_filter(r"peak_mem_used=\d+", "peak_mem_used=<metric>");
    settings.add_filter(r"batches_splitted=\d+", "batches_splitted=<metric>");
    settings.add_filter(r"batches_split=\d+", "batches_split=<metric>");
    settings.add_filter(r"bytes_scanned=\d+", "bytes_scanned=<metric>");
    settings.add_filter(r"file_open_errors=\d+", "file_open_errors=<metric>");
    settings.add_filter(r"file_scan_errors=\d+", "file_scan_errors=<metric>");
    settings.add_filter(
        r"files_ranges_pruned_statistics=\d+",
        "files_ranges_pruned_statistics=<metric>",
    );
    settings.add_filter(
        r"num_predicate_creation_errors=\d+",
        "num_predicate_creation_errors=<metric>",
    );
    settings.add_filter(
        r"page_index_rows_matched=\d+",
        "page_index_rows_matched=<metric>",
    );
    settings.add_filter(
        r"page_index_rows_pruned=\d+",
        "page_index_rows_pruned=<metric>",
    );
    settings.add_filter(
        r"predicate_evaluation_errors=\d+",
        "predicate_evaluation_errors=<metric>",
    );
    settings.add_filter(
        r"pushdown_rows_matched=\d+",
        "pushdown_rows_matched=<metric>",
    );
    settings.add_filter(r"pushdown_rows_pruned=\d+", "pushdown_rows_pruned=<metric>");
    settings.add_filter(
        r"row_groups_matched_bloom_filter=\d+",
        "row_groups_matched_bloom_filter=<metric>",
    );
    settings.add_filter(
        r"row_groups_matched_statistics=\d+",
        "row_groups_matched_statistics=<metric>",
    );
    settings.add_filter(
        r"row_groups_pruned_bloom_filter=\d+",
        "row_groups_pruned_bloom_filter=<metric>",
    );
    settings.add_filter(
        r"row_groups_pruned_statistics=\d+",
        "row_groups_pruned_statistics=<metric>",
    );
    settings.add_filter(
        r"bloom_filter_eval_time=[\d.]+[a-zA-Zµnms]+",
        "bloom_filter_eval_time=<metric>",
    );
    settings.add_filter(
        r"metadata_load_time=[\d.]+[a-zA-Zµnms]+",
        "metadata_load_time=<metric>",
    );
    settings.add_filter(
        r"page_index_eval_time=[\d.]+[a-zA-Zµnms]+",
        "page_index_eval_time=<metric>",
    );
    settings.add_filter(
        r"row_pushdown_eval_time=[\d.]+[a-zA-Zµnms]+",
        "row_pushdown_eval_time=<metric>",
    );
    settings.add_filter(
        r"statistics_eval_time=[\d.]+[a-zA-Zµnms]+",
        "statistics_eval_time=<metric>",
    );
    settings.add_filter(
        r"time_elapsed_opening=[\d.]+[a-zA-Zµnms]+",
        "time_elapsed_opening=<metric>",
    );
    settings.add_filter(
        r"time_elapsed_processing=[\d.]+[a-zA-Zµnms]+",
        "time_elapsed_processing=<metric>",
    );
    settings.add_filter(
        r"time_elapsed_scanning_total=[\d.]+[a-zA-Zµnms]+",
        "time_elapsed_scanning_total=<metric>",
    );
    settings.add_filter(
        r"time_elapsed_scanning_until_data=[\d.]+[a-zA-Zµnms]+",
        "time_elapsed_scanning_until_data=<metric>",
    );
    settings.add_filter(
        r"skipped_aggregation_rows=\d+",
        "skipped_aggregation_rows=<metric>",
    );
    settings.add_filter(r"build_input_batches=\d+", "build_input_batches=<metric>");
    settings.add_filter(r"build_input_rows=\d+", "build_input_rows=<metric>");
    settings.add_filter(r"input_batches=\d+", "input_batches=<metric>");
    settings.add_filter(r"input_rows=\d+", "input_rows=<metric>");
    settings.add_filter(r"output_batches=\d+", "output_batches=<metric>");
    settings.add_filter(r"build_mem_used=\d+", "build_mem_used=<metric>");
    settings.add_filter(r"build_time=[\d.]+[a-zA-Zµnms]+", "build_time=<metric>");
    settings.add_filter(r"join_time=[\d.]+[a-zA-Zµnms]+", "join_time=<metric>");

    settings
}
