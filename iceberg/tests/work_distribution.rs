#[cfg(all(test, feature = "integration"))]
mod tests {
    use datafusion::common::Result;
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::IcebergConfig;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn pruned_files_use_distinct_workers_before_local_partitions() -> Result<()> {
        assert_distribution(
            4,
            16,
            "2024-01-11",
            "output_rows={0:25.00 K, 1:25.00 K, 2:25.00 K, 3:25.00 K}",
            false,
        )
        .await
    }

    #[tokio::test]
    async fn planned_files_use_distinct_workers_before_local_partitions() -> Result<()> {
        assert_distribution(
            4,
            16,
            "2024-01-11",
            "output_rows={0:25.00 K, 1:25.00 K, 2:25.00 K, 3:25.00 K}",
            true,
        )
        .await
    }

    #[tokio::test]
    async fn files_wrap_across_workers_and_local_partitions() -> Result<()> {
        for eager in [false, true] {
            assert_distribution(
                3,
                2,
                "2024-01-14",
                "output_rows={0:75.00 K, 1:50.00 K, 2:50.00 K}",
                eager,
            )
            .await?;
        }
        Ok(())
    }

    async fn assert_distribution(
        workers: usize,
        partitions: usize,
        last_date: &str,
        expected_rows: &str,
        eager_file_planning: bool,
    ) -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_workers(workers)
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = eager_file_planning;
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = partitions;
                session.options_mut().extensions.insert(config);
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
        // Assigned inputs render one scan variant per task; feeds render one scan
        // with all task metrics. Assert the same per-task distribution in either form.
        let mut rows: Vec<_> = plan
            .lines()
            .filter(|line| line.contains("DataSourceExec: format=iceberg"))
            .flat_map(|line| {
                line.split_once("output_rows={")
                    .unwrap()
                    .1
                    .split_once('}')
                    .unwrap()
                    .0
                    .split(", ")
            })
            .collect();
        let mut expected: Vec<_> = expected_rows
            .strip_prefix("output_rows={")
            .unwrap()
            .strip_suffix('}')
            .unwrap()
            .split(", ")
            .collect();
        rows.sort_unstable();
        expected.sort_unstable();
        assert_eq!(rows, expected, "{plan}");
        let (_, local_results) = IcebergTestHarness::new().await?.query(&sql).await?;
        assert_eq!(results, local_results);
        Ok(())
    }
}
