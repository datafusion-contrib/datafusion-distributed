mod common;

mod tests {
    use crate::common::IcebergTestHarness;
    use datafusion::common::Result;
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;

    #[tokio::test]
    async fn pruned_files_use_distinct_workers_before_local_partitions() -> Result<()> {
        assert_distribution(4, 16, "2024-01-11", "lazy_pruned_files", false).await
    }

    #[tokio::test]
    async fn planned_files_use_distinct_workers_before_local_partitions() -> Result<()> {
        assert_distribution(4, 16, "2024-01-11", "eager_pruned_files", true).await
    }

    #[tokio::test]
    async fn files_wrap_across_workers_and_local_partitions() -> Result<()> {
        for (eager, snapshot) in [
            (false, "lazy_wrapping_files"),
            (true, "eager_wrapping_files"),
        ] {
            assert_distribution(3, 2, "2024-01-14", snapshot, eager).await?;
        }
        Ok(())
    }

    async fn assert_distribution(
        workers: usize,
        partitions: usize,
        last_date: &str,
        snapshot: &str,
        eager_file_planning: bool,
    ) -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_table(create_taxi_table().await?)
            .with_workers(workers)
            .configure_iceberg(|config| config.eager_file_planning = eager_file_planning)
            .configure_session(|mut state| {
                let session = state.config().get_or_insert_default();
                session.options_mut().execution.target_partitions = partitions;
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
        let scans = plan
            .lines()
            .filter(|line| line.contains("DataSourceExec: format=iceberg"))
            .collect::<Vec<_>>()
            .join("\n");
        // Keep each scan's per-task row counts; omit timing and unrelated metrics.
        insta::with_settings!({
            filters => [(r"(?m)^.*?(output_rows=\{[^}]*\}).*$", "$1")],
        }, {
            insta::assert_snapshot!(snapshot, scans);
        });
        let (_, local_results) = IcebergTestHarness::builder()
            .with_table(create_taxi_table().await?)
            .build()
            .await?
            .query(&sql)
            .await?;
        assert_eq!(results, local_results);
        Ok(())
    }
}
