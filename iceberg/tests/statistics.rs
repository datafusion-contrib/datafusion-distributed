#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::Statistics;
    use datafusion::common::stats::Precision;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::error::Result;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion_distributed_iceberg::IcebergDataSource;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    // Values from testdata/iceberg/taxi/metadata/v1.metadata.json snapshot summary.
    const TAXI_ROWS: usize = 175_000;
    const TAXI_BYTES: usize = 4_480_382;
    const TAXI_COLUMNS: usize = 13;

    #[tokio::test]
    async fn reports_exact_row_count_and_byte_size_for_full_scan() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let stats = source_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        assert_eq!(stats.total_byte_size, Precision::Exact(TAXI_BYTES));
        Ok(())
    }

    #[tokio::test]
    async fn missing_snapshot_summary_statistics_are_absent() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .edit_current_snapshot_summary(|summary| {
                for key in ["total-records", "total-files-size"] {
                    assert!(summary.additional_properties.remove(key).is_some());
                }
            })
            .build()
            .await?;
        let stats = source_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.num_rows, Precision::Absent);
        assert_eq!(stats.total_byte_size, Precision::Absent);
        Ok(())
    }

    #[tokio::test]
    async fn column_statistics_match_full_schema() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let stats = source_statistics(&harness, "SELECT * FROM taxi").await?;

        assert_eq!(stats.column_statistics.len(), TAXI_COLUMNS);
        Ok(())
    }

    #[tokio::test]
    async fn column_statistics_match_projected_schema() -> Result<()> {
        // Regression: a column_statistics vec shorter than the output schema
        // makes DataFusion panic while propagating statistics upstream.
        let harness = IcebergTestHarness::new().await?;
        let stats = source_statistics(&harness, "SELECT vendor_id, pickup_date FROM taxi").await?;

        assert_eq!(stats.column_statistics.len(), 2);
        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        Ok(())
    }

    #[tokio::test]
    async fn statistics_propagate_through_filter() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness
            .physical_plan("SELECT vendor_id FROM taxi WHERE pickup_date = DATE '2024-01-10'")
            .await?;
        let stats = plan.partition_statistics(None)?;

        // The filter cannot keep the count exact, but it must not lose it.
        assert!(matches!(stats.num_rows, Precision::Inexact(_)));
        assert_eq!(stats.column_statistics.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn statistics_propagate_through_projection_and_sort() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness
            .physical_plan("SELECT vendor_id, trip_distance FROM taxi ORDER BY pickup_at")
            .await?;
        let stats = plan.partition_statistics(None)?;

        assert_eq!(stats.num_rows, Precision::Exact(TAXI_ROWS));
        assert_eq!(stats.column_statistics.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn explain_shows_statistics_on_the_iceberg_source() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let plan = harness.physical_plan("SELECT vendor_id FROM taxi").await?;
        let display = displayable(plan.as_ref())
            .set_show_statistics(true)
            .indent(true)
            .to_string();

        insta::assert_snapshot!(display, @"
        CooperativeExec, statistics=[Rows=Exact(175000), Bytes=Exact(4480382), [(Col[0]:)]]
          DataSourceExec: format=iceberg, projection=[vendor_id], statistics=[Rows=Exact(175000), Bytes=Exact(4480382), [(Col[0]:)]]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn exact_row_count_lets_count_star_skip_the_scan() -> Result<()> {
        // With Precision::Exact(num_rows) the AggregateStatistics optimizer
        // rule answers COUNT(*) from metadata without reading any data file.
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness.query("SELECT count(*) FROM taxi").await?;

        insta::assert_snapshot!(plan, @"
        ProjectionExec: expr=[175000 as count(*)]
          PlaceholderRowExec
        ");
        insta::assert_snapshot!(batches, @"
        +----------+
        | count(*) |
        +----------+
        | 175000   |
        +----------+
        ");
        Ok(())
    }

    /// Finds the single Iceberg `DataSourceExec` in the plan and returns the
    /// statistics reported by the `IcebergDataSource` itself.
    async fn source_statistics(harness: &IcebergTestHarness, sql: &str) -> Result<Statistics> {
        let plan = harness.physical_plan(sql).await?;
        let exec = find_iceberg_exec(&plan).expect("plan contains an Iceberg DataSourceExec");
        Ok(Arc::unwrap_or_clone(exec.partition_statistics(None)?))
    }

    fn find_iceberg_exec(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<DataSourceExec>> {
        if let Some(exec) = plan.downcast_ref::<DataSourceExec>() {
            if exec
                .data_source()
                .downcast_ref::<IcebergDataSource>()
                .is_some()
            {
                return Some(Arc::new(exec.clone()));
            }
        }
        plan.children().into_iter().find_map(find_iceberg_exec)
    }
}
