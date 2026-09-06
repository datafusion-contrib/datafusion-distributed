#[cfg(test)]
mod tests {
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn applies_sql_limit_to_the_iceberg_scan_output() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT vendor_id, pickup_at, passenger_count, trip_distance, pickup_date
               FROM taxi
               WHERE pickup_date = DATE '2024-01-10'
               ORDER BY pickup_at
               LIMIT 3",
            )
            .await?;

        insta::assert_snapshot!(plan, @r"
        SortPreservingMergeExec: [pickup_at@1 ASC NULLS LAST], fetch=3
          SortExec: TopK(fetch=3), expr=[pickup_at@1 ASC NULLS LAST], preserve_partitioning=[true]
            FilterExec: pickup_date@4 = 2024-01-10
              DataSourceExec: format=iceberg, projection=[vendor_id, pickup_at, passenger_count, trip_distance, pickup_date], predicate=pickup_date = 2024-01-10
        ");
        insta::assert_snapshot!(batches, @r"
    +-----------+---------------------+-----------------+---------------+-------------+
    | vendor_id | pickup_at           | passenger_count | trip_distance | pickup_date |
    +-----------+---------------------+-----------------+---------------+-------------+
    | 2         | 2024-01-10T00:00:09 |                 | 0.78          | 2024-01-10  |
    | 2         | 2024-01-10T00:00:10 | 1               | 0.88          | 2024-01-10  |
    | 1         | 2024-01-10T00:00:10 | 1               | 3.4           | 2024-01-10  |
    +-----------+---------------------+-----------------+---------------+-------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn keeps_unordered_limits_above_the_iceberg_source() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query("SELECT pickup_date FROM taxi WHERE pickup_date = DATE '2024-01-10' LIMIT 3")
            .await?;

        insta::assert_snapshot!(plan, @r"
        CoalescePartitionsExec: fetch=3
          FilterExec: pickup_date@0 = 2024-01-10, fetch=3
            DataSourceExec: format=iceberg, projection=[pickup_date], predicate=pickup_date = 2024-01-10
        ");
        insta::assert_snapshot!(batches, @r"
    +-------------+
    | pickup_date |
    +-------------+
    | 2024-01-10  |
    | 2024-01-10  |
    | 2024-01-10  |
    +-------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn passes_the_scan_limit_to_the_iceberg_source() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query("SELECT pickup_date FROM taxi LIMIT 3")
            .await?;

        insta::assert_snapshot!(plan, @r"
    CoalescePartitionsExec: fetch=3
      DataSourceExec: format=iceberg, projection=[pickup_date], fetch=3
    ");
        insta::assert_snapshot!(batches, @r"
    +-------------+
    | pickup_date |
    +-------------+
    | 2024-01-08  |
    | 2024-01-08  |
    | 2024-01-08  |
    +-------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn avoids_scanning_for_limit_zero() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query_raw("SELECT vendor_id FROM taxi WHERE pickup_date = DATE '2024-01-10' LIMIT 0")
            .await?;

        assert_eq!(plan.name(), "EmptyExec");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn applies_offset_before_limit() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT vendor_id, pickup_at
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                   ORDER BY pickup_at, vendor_id, pickup_location_id
                   LIMIT 2 OFFSET 3",
            )
            .await?;

        insta::assert_snapshot!(plan, @r"
        ProjectionExec: expr=[vendor_id@0 as vendor_id, pickup_at@1 as pickup_at]
          GlobalLimitExec: skip=3, fetch=2
            SortPreservingMergeExec: [pickup_at@1 ASC NULLS LAST, vendor_id@0 ASC NULLS LAST, pickup_location_id@2 ASC NULLS LAST], fetch=5
              SortExec: TopK(fetch=5), expr=[pickup_at@1 ASC NULLS LAST, vendor_id@0 ASC NULLS LAST, pickup_location_id@2 ASC NULLS LAST], preserve_partitioning=[true]
                FilterExec: pickup_date@3 = 2024-01-10, projection=[vendor_id@0, pickup_at@1, pickup_location_id@2]
                  DataSourceExec: format=iceberg, projection=[vendor_id, pickup_at, pickup_location_id, pickup_date], predicate=pickup_date = 2024-01-10
        ");
        insta::assert_snapshot!(batches, @r"
    +-----------+---------------------+
    | vendor_id | pickup_at           |
    +-----------+---------------------+
    | 2         | 2024-01-10T00:00:12 |
    | 2         | 2024-01-10T00:00:14 |
    +-----------+---------------------+
    ");

        Ok(())
    }
}
