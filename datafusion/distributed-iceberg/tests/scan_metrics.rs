mod tests {
    use datafusion::common::Result;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion_distributed::test_utils::metrics::{assert_metrics_equal, node_metrics};
    use datafusion_distributed::{DistributedExt, DistributedLeafExec};
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::IcebergTestHarness;
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;

    #[tokio::test]
    async fn lazy_planning_scan_metrics_match_a_single_node() -> Result<()> {
        let query = "SELECT pickup_date, COUNT(*) FROM taxi \
                     WHERE pickup_date <= DATE '2024-01-10' GROUP BY pickup_date";
        let single_node = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .build()
            .await?
            .plan_with_metrics(query)
            .await?;
        let distributed = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_workers(2)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?
            .plan_with_metrics(query)
            .await?;

        assert_eq!(
            node_metrics::<DataSourceExec>(&single_node, "files_opened", 0),
            3
        );
        assert_metrics_equal::<DataSourceExec, DataSourceExec>(
            ["files_opened", "bytes_scanned"],
            &single_node,
            &distributed,
            0,
        );
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_scan_metrics_match_a_single_node() -> Result<()> {
        let query = "SELECT pickup_date, COUNT(*) FROM taxi \
                     WHERE pickup_date <= DATE '2024-01-10' GROUP BY pickup_date";
        let single_node = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .build()
            .await?
            .plan_with_metrics(query)
            .await?;
        let distributed = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_workers(2)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 2;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?
            .plan_with_metrics(query)
            .await?;

        assert_eq!(
            node_metrics::<DataSourceExec>(&single_node, "files_opened", 0),
            3
        );
        assert_metrics_equal::<DataSourceExec, DistributedLeafExec>(
            ["files_opened", "bytes_scanned"],
            &single_node,
            &distributed,
            0,
        );
        Ok(())
    }

    #[tokio::test]
    async fn bytes_scanned_counts_files_that_return_no_rows() -> Result<()> {
        let plan = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .build()
            .await?
            .plan_with_metrics(
                "SELECT * FROM taxi \
                 WHERE pickup_date <= DATE '2024-01-10' AND fare_amount = 12.345",
            )
            .await?;

        assert_eq!(node_metrics::<DataSourceExec>(&plan, "files_opened", 0), 3);
        assert!(node_metrics::<DataSourceExec>(&plan, "bytes_scanned", 0) > 0);
        Ok(())
    }

    #[tokio::test]
    async fn lazy_planning_limit_counts_only_the_files_the_reader_opened() -> Result<()> {
        let plan = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", false)
            .with_config_option("iceberg.data_file_concurrency_limit", 1)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                Ok(state)
            })?
            .build()
            .await?
            .plan_with_metrics("SELECT * FROM taxi LIMIT 1")
            .await?;

        assert_eq!(node_metrics::<DataSourceExec>(&plan, "files_opened", 0), 1);
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_limit_counts_only_the_files_the_reader_opened() -> Result<()> {
        let plan = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .with_config_option("iceberg.eager_file_planning", true)
            .with_config_option("iceberg.data_file_concurrency_limit", 1)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = 1;
                Ok(state)
            })?
            .build()
            .await?
            .plan_with_metrics("SELECT * FROM taxi LIMIT 1")
            .await?;

        assert_eq!(node_metrics::<DataSourceExec>(&plan, "files_opened", 0), 1);
        Ok(())
    }
}
