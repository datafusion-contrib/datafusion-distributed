mod common;

#[cfg(test)]
mod tests {
    use datafusion::error::Result;
    use datafusion::physical_plan::displayable;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    use crate::common::assert_scalar_result;

    #[tokio::test]
    async fn pushes_down_compound_predicates() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query_raw(
                r"SELECT COUNT(*) AS trips
               FROM taxi
               WHERE pickup_date = DATE '2024-01-10'
                 AND payment_type IN (1, 2)
                 AND trip_distance >= 2.0",
            )
            .await?;

        insta::assert_snapshot!(displayable(plan.as_ref()).indent(true), @r"
    ProjectionExec: expr=[count(Int64(1))@0 as trips]
      AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        CoalescePartitionsExec
          AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
            FilterExec: pickup_date@2 = 2024-01-10 AND (payment_type@1 = 1 OR payment_type@1 = 2) AND trip_distance@0 >= 2, projection=[]
              DataSourceExec: format=iceberg, projection=[trip_distance, payment_type, pickup_date], predicate=((pickup_date = 2024-01-10) AND ((payment_type = 1) OR (payment_type = 2))) AND (trip_distance >= 2)
    ");
        assert_scalar_result(&batches, "trips", 9891_i64.into())
    }

    #[tokio::test]
    async fn pushes_down_disjunctions() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query(
                r"SELECT pickup_date, COUNT(*) AS trips
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                      OR pickup_date = DATE '2024-01-11'
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
                    FilterExec: pickup_date@0 = 2024-01-10 OR pickup_date@0 = 2024-01-11
                      DataSourceExec: format=iceberg, projection=[pickup_date], predicate=(pickup_date = 2024-01-10) OR (pickup_date = 2024-01-11)
        ");
        insta::assert_snapshot!(batches, @r"
    +-------------+-------+
    | pickup_date | trips |
    +-------------+-------+
    | 2024-01-10  | 25000 |
    | 2024-01-11  | 25000 |
    +-------------+-------+
    ");

        Ok(())
    }

    #[tokio::test]
    async fn pushes_down_null_predicates() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query_raw("SELECT COUNT(*) AS trips FROM taxi WHERE passenger_count IS NULL")
            .await?;

        insta::assert_snapshot!(displayable(plan.as_ref()).indent(true), @r"
    ProjectionExec: expr=[count(Int64(1))@0 as trips]
      AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        CoalescePartitionsExec
          AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
            FilterExec: passenger_count@0 IS NULL, projection=[]
              DataSourceExec: format=iceberg, projection=[passenger_count], predicate=passenger_count IS NULL
    ");
        assert_scalar_result(&batches, "trips", 8829_i64.into())
    }

    #[tokio::test]
    async fn retains_unsupported_filter_after_safe_pushdown() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query_raw(
                r"SELECT COUNT(*) AS trips
                   FROM taxi
                   WHERE pickup_date = DATE '2024-01-10'
                     AND trip_distance + 1.0 > 3.0",
            )
            .await?;

        insta::assert_snapshot!(displayable(plan.as_ref()).indent(true), @r"
    ProjectionExec: expr=[count(Int64(1))@0 as trips]
      AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        CoalescePartitionsExec
          AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
            FilterExec: pickup_date@1 = 2024-01-10 AND trip_distance@0 + 1 > 3, projection=[]
              DataSourceExec: format=iceberg, projection=[trip_distance, pickup_date], predicate=pickup_date = 2024-01-10
    ");
        assert_scalar_result(&batches, "trips", 10_516_i64.into())
    }

    #[tokio::test]
    async fn executes_wholly_unsupported_filters_without_pushdown() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, batches) = harness
            .query_raw("SELECT COUNT(*) AS trips FROM taxi WHERE trip_distance + 1.0 > 20.0")
            .await?;

        insta::assert_snapshot!(displayable(plan.as_ref()).indent(true), @r"
    ProjectionExec: expr=[count(Int64(1))@0 as trips]
      AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        CoalescePartitionsExec
          AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
            FilterExec: trip_distance@0 + 1 > 20, projection=[]
              DataSourceExec: format=iceberg, projection=[trip_distance]
    ");
        assert_scalar_result(&batches, "trips", 2757_i64.into())
    }
}
