mod tests {
    use datafusion::error::Result;
    use datafusion::physical_plan::collect;
    use datafusion_distributed::display_plan_ascii;
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg_test_tools::IcebergTestHarness;
    use datafusion_distributed_iceberg_test_tools::create_taxi_table;

    #[tokio::test]
    async fn applies_sql_limit_to_the_iceberg_scan_output() -> Result<()> {
        let (plan, batches) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .query(
                r"SELECT vendor_id, pickup_at, passenger_count, trip_distance, pickup_date
               FROM taxi
               WHERE pickup_date = DATE '2024-01-10'
               ORDER BY pickup_at
               LIMIT 3",
            )
            .await?;

        insta::assert_snapshot!(plan, @"
        SortPreservingMergeExec: [pickup_at@1 ASC NULLS LAST], fetch=3
          SortExec: TopK(fetch=3), expr=[pickup_at@1 ASC NULLS LAST], preserve_partitioning=[true]
            FilterExec: pickup_date@4 = 2024-01-10
              DataSourceExec: format=iceberg, table=default.taxi, projection=[vendor_id, pickup_at, passenger_count, trip_distance, pickup_date], predicate=pickup_date = 2024-01-10
        ");
        insta::assert_snapshot!(batches, @r"
        +-----------+---------------------+-----------------+---------------+-------------+
        | vendor_id | pickup_at           | passenger_count | trip_distance | pickup_date |
        +-----------+---------------------+-----------------+---------------+-------------+
        | 1         | 2024-01-10T00:00:04 | 1               | 0.68          | 2024-01-10  |
        | 2         | 2024-01-10T00:00:08 | 1               | 6.18          | 2024-01-10  |
        | 2         | 2024-01-10T00:00:11 |                 | 3.61          | 2024-01-10  |
        +-----------+---------------------+-----------------+---------------+-------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn keeps_unordered_limits_above_the_iceberg_source() -> Result<()> {
        let (plan, batches) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .query("SELECT pickup_date FROM taxi WHERE pickup_date = DATE '2024-01-10' LIMIT 3")
            .await?;

        insta::assert_snapshot!(plan, @"
        CoalescePartitionsExec: fetch=3
          FilterExec: pickup_date@0 = 2024-01-10, fetch=3
            DataSourceExec: format=iceberg, table=default.taxi, projection=[pickup_date], predicate=pickup_date = 2024-01-10
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
        let harness = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .build()
            .await?;
        let plan = harness
            .physical_plan("SELECT pickup_date FROM taxi LIMIT 3")
            .await?;

        insta::assert_snapshot!(display_plan_ascii(plan.as_ref(), false), @"
        CoalescePartitionsExec: fetch=3
          DataSourceExec: format=iceberg, table=default.taxi, projection=[pickup_date], fetch=3
        ");
        // Without an ORDER BY, any three rows are correct: which file's rows arrive first
        // depends on how the parallel reads are scheduled.
        let batches = collect(plan, harness.state().task_ctx()).await?;
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            3
        );

        Ok(())
    }

    #[tokio::test]
    async fn avoids_scanning_for_limit_zero() -> Result<()> {
        let (plan, batches) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .query("SELECT vendor_id FROM taxi WHERE pickup_date = DATE '2024-01-10' LIMIT 0")
            .await?;

        insta::assert_snapshot!(plan, @r"
    EmptyExec
    ");
        insta::assert_snapshot!(batches, @r"
    ++
    ++
    ");

        Ok(())
    }

    #[tokio::test]
    async fn applies_offset_before_limit() -> Result<()> {
        let (plan, batches) = IcebergTestHarness::builder()
            .with_session_extension(|state| state.with_iceberg_integration(Default::default()))
            .with_table(create_taxi_table().await?)
            .query(
                r"SELECT vendor_id, pickup_at
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                   ORDER BY pickup_at, vendor_id, pickup_location_id
                   LIMIT 2 OFFSET 3",
            )
            .await?;

        insta::assert_snapshot!(plan, @"
        ProjectionExec: expr=[vendor_id@0 as vendor_id, pickup_at@1 as pickup_at]
          GlobalLimitExec: skip=3, fetch=2
            SortPreservingMergeExec: [pickup_at@1 ASC NULLS LAST, vendor_id@0 ASC NULLS LAST, pickup_location_id@2 ASC NULLS LAST], fetch=5
              SortExec: TopK(fetch=5), expr=[pickup_at@1 ASC NULLS LAST, vendor_id@0 ASC NULLS LAST, pickup_location_id@2 ASC NULLS LAST], preserve_partitioning=[true]
                FilterExec: pickup_date@3 = 2024-01-10, projection=[vendor_id@0, pickup_at@1, pickup_location_id@2]
                  DataSourceExec: format=iceberg, table=default.taxi, projection=[vendor_id, pickup_at, pickup_location_id, pickup_date], predicate=pickup_date = 2024-01-10
        ");
        insta::assert_snapshot!(batches, @r"
        +-----------+---------------------+
        | vendor_id | pickup_at           |
        +-----------+---------------------+
        | 1         | 2024-01-10T00:00:15 |
        | 2         | 2024-01-10T00:00:18 |
        +-----------+---------------------+
        ");

        Ok(())
    }
}
