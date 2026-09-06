mod common;

#[cfg(test)]
mod tests {
    use datafusion::error::Result;
    use datafusion::physical_plan::displayable;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    use crate::common::assert_scalar_result;

    #[tokio::test]
    async fn projects_only_columns_required_by_the_query() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT pickup_date, COUNT(*) AS trips
               FROM taxi
               WHERE pickup_date >= DATE '2024-01-10'
               GROUP BY pickup_date
               ORDER BY pickup_date",
            )
            .await?;

        insta::assert_snapshot!(plan, @r"
        SortPreservingMergeExec: [pickup_date@0 ASC NULLS LAST]
          SortExec: expr=[pickup_date@0 ASC NULLS LAST], preserve_partitioning=[true]
            ProjectionExec: expr=[pickup_date@0 as pickup_date, count(Int64(1))@1 as trips]
              AggregateExec: mode=FinalPartitioned, gby=[pickup_date@0 as pickup_date], aggr=[count(Int64(1))]
                RepartitionExec: partitioning=Hash([pickup_date@0], 4), input_partitions=4
                  AggregateExec: mode=Partial, gby=[pickup_date@0 as pickup_date], aggr=[count(Int64(1))]
                    FilterExec: pickup_date@0 >= 2024-01-10
                      DataSourceExec: format=iceberg, projection=[pickup_date], predicate=pickup_date >= 2024-01-10
        ");
        insta::assert_snapshot!(batches, @r"
    +-------------+-------+
    | pickup_date | trips |
    +-------------+-------+
    | 2024-01-10  | 25000 |
    | 2024-01-11  | 25000 |
    | 2024-01-12  | 25000 |
    | 2024-01-13  | 25000 |
    | 2024-01-14  | 25000 |
    +-------------+-------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn includes_filter_columns_that_are_not_in_the_output() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT vendor_id, pickup_location_id
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                   ORDER BY pickup_at, vendor_id
                   LIMIT 3",
            )
            .await?;

        insta::assert_snapshot!(plan, @r"
    ProjectionExec: expr=[vendor_id@0 as vendor_id, pickup_location_id@1 as pickup_location_id]
      SortPreservingMergeExec: [pickup_at@2 ASC NULLS LAST, vendor_id@0 ASC NULLS LAST], fetch=3
        SortExec: TopK(fetch=3), expr=[pickup_at@2 ASC NULLS LAST, vendor_id@0 ASC NULLS LAST], preserve_partitioning=[true]
          FilterExec: pickup_date@3 = 2024-01-10, projection=[vendor_id@0, pickup_location_id@2, pickup_at@1]
            DataSourceExec: format=iceberg, projection=[vendor_id, pickup_at, pickup_location_id, pickup_date], predicate=pickup_date = 2024-01-10
    ");
        insta::assert_snapshot!(batches, @r"
    +-----------+--------------------+
    | vendor_id | pickup_location_id |
    +-----------+--------------------+
    | 2         | 75                 |
    | 1         | 161                |
    | 2         | 162                |
    +-----------+--------------------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn projects_source_columns_for_computed_expressions() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query_raw(
                r"SELECT MAX(trip_distance * fare_amount) AS max_weighted_fare
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'",
            )
            .await?;

        insta::assert_snapshot!(displayable(plan.as_ref()).indent(true), @r"
    ProjectionExec: expr=[max(taxi.trip_distance * taxi.fare_amount)@0 as max_weighted_fare]
      AggregateExec: mode=Final, gby=[], aggr=[max(taxi.trip_distance * taxi.fare_amount)]
        CoalescePartitionsExec
          AggregateExec: mode=Partial, gby=[], aggr=[max(taxi.trip_distance * taxi.fare_amount)]
            FilterExec: pickup_date@2 = 2024-01-10, projection=[trip_distance@0, fare_amount@1]
              DataSourceExec: format=iceberg, projection=[trip_distance, fare_amount, pickup_date], predicate=pickup_date = 2024-01-10
    ");
        assert_scalar_result(&batches, "max_weighted_fare", 207_508.958_4_f64.into())
    }

    #[tokio::test]
    async fn reads_all_columns_when_selecting_star() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT *
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                   ORDER BY pickup_at, vendor_id, pickup_location_id
                   LIMIT 1",
            )
            .await?;

        insta::assert_snapshot!(plan, @r"
    SortPreservingMergeExec: [pickup_at@1 ASC NULLS LAST, vendor_id@0 ASC NULLS LAST, pickup_location_id@5 ASC NULLS LAST], fetch=1
      SortExec: TopK(fetch=1), expr=[pickup_at@1 ASC NULLS LAST, vendor_id@0 ASC NULLS LAST, pickup_location_id@5 ASC NULLS LAST], preserve_partitioning=[true]
        FilterExec: pickup_date@12 = 2024-01-10
          DataSourceExec: format=iceberg, projection=[vendor_id, pickup_at, dropoff_at, passenger_count, trip_distance, pickup_location_id, dropoff_location_id, payment_type, fare_amount, tip_amount, tolls_amount, total_amount, pickup_date], predicate=pickup_date = 2024-01-10
    ");
        insta::assert_snapshot!(batches, @r"
    +-----------+---------------------+---------------------+-----------------+---------------+--------------------+---------------------+--------------+-------------+------------+--------------+--------------+-------------+
    | vendor_id | pickup_at           | dropoff_at          | passenger_count | trip_distance | pickup_location_id | dropoff_location_id | payment_type | fare_amount | tip_amount | tolls_amount | total_amount | pickup_date |
    +-----------+---------------------+---------------------+-----------------+---------------+--------------------+---------------------+--------------+-------------+------------+--------------+--------------+-------------+
    | 2         | 2024-01-10T00:00:09 | 2024-01-10T00:03:30 |                 | 0.78          | 75                 | 236                 | 0            | 1.74        | 3.15       | 0.0          | 8.89         | 2024-01-10  |
    +-----------+---------------------+---------------------+-----------------+---------------+--------------------+---------------------+--------------+-------------+------------+--------------+--------------+-------------+
    ");

        Ok(())
    }
}
