#[cfg(test)]
mod tests {
    use datafusion::common::Result;
    use datafusion::execution::SessionStateBuilder;
    #[cfg(feature = "integration")]
    use datafusion_distributed::DistributedExt;
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg::test_utils::{
        IcebergTestHarness, IcebergTestHarnessBuilder,
    };

    const TRIPS_PER_DATE: &str = "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
         GROUP BY pickup_date ORDER BY pickup_date";

    const TRIPS_PER_LABEL: &str = "SELECT d.label, COUNT(*) AS trips \
         FROM taxi t \
         JOIN (VALUES \
            (DATE '2024-01-08', 'a'), (DATE '2024-01-09', 'b'), (DATE '2024-01-10', 'c'), \
            (DATE '2024-01-11', 'd'), (DATE '2024-01-12', 'e'), (DATE '2024-01-13', 'f'), \
            (DATE '2024-01-14', 'g')) AS d(pickup_date, label) \
         ON t.pickup_date = d.pickup_date \
         GROUP BY d.label ORDER BY d.label";

    #[tokio::test]
    async fn is_disabled_by_default() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let (plan, _) = harness.query(TRIPS_PER_DATE).await?;

        insta::assert_snapshot!(plan, @"
        SortPreservingMergeExec: [pickup_date@0 ASC NULLS LAST]
          ProjectionExec: expr=[pickup_date@0 as pickup_date, count(Int64(1))@1 as trips]
            SortExec: expr=[pickup_date@0 ASC NULLS LAST], preserve_partitioning=[true]
              AggregateExec: mode=FinalPartitioned, gby=[pickup_date@0 as pickup_date], aggr=[count(Int64(1))]
                RepartitionExec: partitioning=Hash([pickup_date@0], 4), input_partitions=4
                  AggregateExec: mode=Partial, gby=[pickup_date@0 as pickup_date], aggr=[count(Int64(1))]
                    DataSourceExec: format=iceberg, projection=[pickup_date]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn removes_the_repartition_on_identity_partition_columns() -> Result<()> {
        let harness = hash_partitioned().build().await?;
        let (plan, batches) = harness.query(TRIPS_PER_DATE).await?;

        insta::assert_snapshot!(plan + &batches, @"
        SortPreservingMergeExec: [pickup_date@0 ASC NULLS LAST]
          ProjectionExec: expr=[pickup_date@0 as pickup_date, count(Int64(1))@1 as trips]
            SortExec: expr=[pickup_date@0 ASC NULLS LAST], preserve_partitioning=[true]
              AggregateExec: mode=SinglePartitioned, gby=[pickup_date@0 as pickup_date], aggr=[count(Int64(1))]
                DataSourceExec: format=iceberg, hash_partitioning=[pickup_date@0], projection=[pickup_date]
        +-------------+-------+
        | pickup_date | trips |
        +-------------+-------+
        | 2024-01-08  | 25000 |
        | 2024-01-09  | 25000 |
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
    async fn keeps_the_repartition_on_other_columns() -> Result<()> {
        let harness = hash_partitioned().build().await?;
        let (plan, _) = harness
            .query("SELECT vendor_id, COUNT(*) AS trips FROM taxi GROUP BY vendor_id")
            .await?;

        insta::assert_snapshot!(plan, @"
        ProjectionExec: expr=[vendor_id@0 as vendor_id, count(Int64(1))@1 as trips]
          AggregateExec: mode=FinalPartitioned, gby=[vendor_id@0 as vendor_id], aggr=[count(Int64(1))]
            RepartitionExec: partitioning=Hash([vendor_id@0], 4), input_partitions=4
              AggregateExec: mode=Partial, gby=[vendor_id@0 as vendor_id], aggr=[count(Int64(1))]
                DataSourceExec: format=iceberg, projection=[vendor_id]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn is_co_partitioned_with_a_hash_repartitioned_join_side() -> Result<()> {
        let harness = hash_partitioned()
            .configure_session(force_partitioned_joins)?
            .build()
            .await?;
        let (plan, batches) = harness.query(TRIPS_PER_LABEL).await?;

        insta::assert_snapshot!(plan + &batches, @"
        SortPreservingMergeExec: [label@0 ASC NULLS LAST]
          ProjectionExec: expr=[label@0 as label, count(Int64(1))@1 as trips]
            SortExec: expr=[label@0 ASC NULLS LAST], preserve_partitioning=[true]
              AggregateExec: mode=FinalPartitioned, gby=[label@0 as label], aggr=[count(Int64(1))]
                RepartitionExec: partitioning=Hash([label@0], 4), input_partitions=4
                  AggregateExec: mode=Partial, gby=[label@0 as label], aggr=[count(Int64(1))]
                    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(pickup_date@0, pickup_date@0)], projection=[label@1]
                      RepartitionExec: partitioning=Hash([pickup_date@0], 4), input_partitions=1
                        ProjectionExec: expr=[column1@0 as pickup_date, column2@1 as label]
                          DataSourceExec: partitions=1, partition_sizes=[1]
                      DataSourceExec: format=iceberg, hash_partitioning=[pickup_date@0], projection=[pickup_date]
        +-------+-------+
        | label | trips |
        +-------+-------+
        | a     | 25000 |
        | b     | 25000 |
        | c     | 25000 |
        | d     | 25000 |
        | e     | 25000 |
        | f     | 25000 |
        | g     | 25000 |
        +-------+-------+
        ");
        Ok(())
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn removes_the_network_shuffle_on_identity_partition_columns() -> Result<()> {
        let harness = distributed_hash_partitioned()?.build().await?;
        let (plan, batches) = harness.query(TRIPS_PER_DATE).await?;

        insta::assert_snapshot!(plan + &batches, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [pickup_date@0 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=3, partitions=2
          │ ProjectionExec: expr=[pickup_date@0 as pickup_date, count(Int64(1))@1 as trips]
          │   SortExec: expr=[pickup_date@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=SinglePartitioned, gby=[pickup_date@0 as pickup_date], aggr=[count(Int64(1))]
          │       DataSourceExec: format=iceberg, hash_partitioning=[pickup_date@0], projection=[pickup_date]
          └──────────────────────────────────────────────────
        +-------------+-------+
        | pickup_date | trips |
        +-------------+-------+
        | 2024-01-08  | 25000 |
        | 2024-01-09  | 25000 |
        | 2024-01-10  | 25000 |
        | 2024-01-11  | 25000 |
        | 2024-01-12  | 25000 |
        | 2024-01-13  | 25000 |
        | 2024-01-14  | 25000 |
        +-------------+-------+
        ");
        Ok(())
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn is_co_partitioned_with_a_network_shuffled_join_side() -> Result<()> {
        let harness = distributed_hash_partitioned()?
            .configure_session(force_partitioned_joins)?
            .build()
            .await?;
        let (plan, batches) = harness.query(TRIPS_PER_LABEL).await?;

        insta::assert_snapshot!(plan + &batches, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [label@0 ASC NULLS LAST]
        │   [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=3, partitions=2
          │ ProjectionExec: expr=[label@0 as label, count(Int64(1))@1 as trips]
          │   SortExec: expr=[label@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[label@0 as label], aggr=[count(Int64(1))]
          │       [Stage 2] => NetworkShuffleExec: output_partitions=2, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=3, partitions=6
            │ RepartitionExec: partitioning=Hash([label@0], 6), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[label@0 as label], aggr=[count(Int64(1))]
            │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(pickup_date@0, pickup_date@0)], projection=[label@1]
            │       [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=1
            │       DataSourceExec: format=iceberg, hash_partitioning=[pickup_date@0], projection=[pickup_date]
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=1, partitions=6
              │ RepartitionExec: partitioning=Hash([pickup_date@0], 6), input_partitions=1
              │   ProjectionExec: expr=[column1@0 as pickup_date, column2@1 as label]
              │     DataSourceExec: partitions=1, partition_sizes=[1]
              └──────────────────────────────────────────────────
        +-------+-------+
        | label | trips |
        +-------+-------+
        | a     | 25000 |
        | b     | 25000 |
        | c     | 25000 |
        | d     | 25000 |
        | e     | 25000 |
        | f     | 25000 |
        | g     | 25000 |
        +-------+-------+
        ");
        Ok(())
    }

    /// Creating session with partitioning enabled (supporting identity transforms for now)
    fn hash_partitioned() -> IcebergTestHarnessBuilder {
        IcebergTestHarness::builder()
            .configure_session(|state| Ok(state.with_iceberg_hash_partitioning_enabled(true)))
            .expect("the session configuration is infallible")
    }

    /// Forces partitioned joins, so that the other side of the join gets hash repartitioned.
    fn force_partitioned_joins(mut state: SessionStateBuilder) -> Result<SessionStateBuilder> {
        let config = state.config().get_or_insert_default();
        let optimizer = &mut config.options_mut().optimizer;
        optimizer.hash_join_single_partition_threshold = 0;
        optimizer.hash_join_single_partition_threshold_rows = 0;
        Ok(state)
    }

    /// 4,480,382 bytes / 1 MB / 2 partitions rounds up to 3 tasks.
    #[cfg(feature = "integration")]
    fn distributed_hash_partitioned() -> Result<IcebergTestHarnessBuilder> {
        hash_partitioned()
            .with_workers(4)
            .configure_session(|mut state| {
                let config = state.config().get_or_insert_default();
                config.options_mut().execution.target_partitions = 2;
                state.with_distributed_file_scan_config_bytes_per_partition(1_000_000)
            })
    }
}
