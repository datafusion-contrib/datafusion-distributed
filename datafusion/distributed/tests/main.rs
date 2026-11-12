//! All integration tests compile into this single test binary. Every file in `tests/` is a
//! module here, so adding a new test file requires adding a `mod` line below.

mod chaos;
mod clickbench_correctness_test;
mod clickbench_plans_test;
mod custom_config_extension;
mod custom_extension_codec;
mod distributed_aggregation;
mod distributed_unions;
mod dynamic_filtering;
mod empty_build_partitions;
mod empty_columns_between_workers;
mod error_propagation;
mod explain_analyze;
mod introspection;
mod join;
mod local_connections;
mod metrics_collection;
mod multi_task_collect_join_repros;
mod passthrough_headers;
mod shuffle;
mod sorted_data_source;
mod stateful_data_cleanup;
mod stateful_execution_plan;
mod task_estimator_test;
mod task_finished_handler;
mod tpcds_correctness_test;
mod tpcds_plans_test;
mod tpch_correctness_test;
mod tpch_plans_test;
mod tpch_sorted_correctness_test;
mod tpch_sorted_plans_test;
mod udfs;
mod window;
mod work_unit_feed;
mod worker_plan_hook;
