#[cfg(all(test, feature = "integration"))]
mod tests {
    use datafusion::common::Result;
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn pruned_files_use_distinct_workers_before_local_partitions() -> Result<()> {
        assert_distribution(
            4,
            16,
            "2024-01-11",
            "output_rows={0:25.00 K, 1:25.00 K, 2:25.00 K, 3:25.00 K}",
        )
        .await
    }

    #[tokio::test]
    async fn files_wrap_across_workers_and_local_partitions() -> Result<()> {
        assert_distribution(
            3,
            2,
            "2024-01-14",
            "output_rows={0:75.00 K, 1:50.00 K, 2:50.00 K}",
        )
        .await
    }

    async fn assert_distribution(
        workers: usize,
        partitions: usize,
        last_date: &str,
        expected_rows: &str,
    ) -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_workers(workers)
            .configure_session(|mut state| {
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .execution
                    .target_partitions = partitions;
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let sql = format!(
            "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
             WHERE pickup_date <= DATE '{last_date}' \
             GROUP BY pickup_date ORDER BY pickup_date"
        );
        let (plan, results) = harness.query_with_metrics(&sql).await?;
        let scan = plan
            .lines()
            .find(|line| line.contains("DataSourceExec: format=iceberg"))
            .expect("query must execute an Iceberg scan");
        assert!(scan.contains(expected_rows), "{plan}");
        let (_, local_results) = IcebergTestHarness::new().await?.query(&sql).await?;
        assert_eq!(results, local_results);
        Ok(())
    }
}
