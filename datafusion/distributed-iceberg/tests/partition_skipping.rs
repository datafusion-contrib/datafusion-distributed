mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::collect;
    use datafusion_distributed::test_utils::metrics::node_metrics;
    use datafusion_distributed::{
        DistributedExt, DistributedLeafExec, DistributedMetricsFormat,
        rewrite_distributed_plan_with_metrics,
    };
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::IcebergTestHarness;
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;
    use std::sync::Arc;

    // The taxi table has one file per day, for the 7 days from 2024-01-08. The join keeps two of
    // them, so its filter lets the scan skip the other 5 files.
    const TWO_DAYS_OF_TRIPS: &str = "SELECT COUNT(*) AS trips FROM taxi \
        JOIN (VALUES (DATE '2024-01-10'), (DATE '2024-01-12')) AS days(day) \
        ON taxi.pickup_date = days.day";

    #[tokio::test]
    async fn lazy_planning_workers_skip_files_the_join_filter_rules_out() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .with_workers(2)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;

        let plan = harness.physical_plan(TWO_DAYS_OF_TRIPS).await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------+
        | trips |
        +-------+
        | 50000 |
        +-------+
        ");
        // The first `DataSourceExec` is the join's `VALUES` table. The second is the taxi scan.
        assert_eq!(node_metrics::<DataSourceExec>(&plan, "files_opened", 1), 2);
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_workers_skip_files_the_join_filter_rules_out() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.dynamic_filtering_enabled", true)
            .with_workers(2)
            .configure_session(|state| {
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;

        let plan = harness.physical_plan(TWO_DAYS_OF_TRIPS).await?;
        let batches = collect(Arc::clone(&plan), harness.state().task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        insta::assert_snapshot!(pretty_format_batches(&batches)?, @"
        +-------+
        | trips |
        +-------+
        | 50000 |
        +-------+
        ");
        assert_eq!(
            node_metrics::<DistributedLeafExec>(&plan, "files_opened", 0),
            2
        );
        Ok(())
    }
}
