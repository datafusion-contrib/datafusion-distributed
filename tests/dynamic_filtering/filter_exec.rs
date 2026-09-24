#[cfg(test)]
mod tests {
    use crate::common::TestQuery;
    use datafusion::common::Result;
    use datafusion_distributed::assert_snapshot;

    /// A broadcast join updates the FilterExec beneath its probe-side sort.
    #[tokio::test]
    async fn local_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                WITH counts AS (
                    SELECT COUNT(*) AS n FROM weather GROUP BY "RainToday"
                )
                SELECT r.n FROM counts l JOIN counts r ON l.n = r.n
                ORDER BY r.n
            "#,
        )
        .with_broadcast_joins()
        .with_expected_rows(2)
        .execute()
        .await?;
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [n@0 ASC NULLS LAST]
        │   [Stage 4] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 4 ── tasks=2, partitions=3
          │ HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n@0, n@0)], projection=[n@1]
          │   CoalescePartitionsExec
          │     [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=2
          │   SortExec: expr=[n@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     FilterExec: task_variants={t0: [FilterExec: DynamicFilter [ expression_id_0_hash_0 ]], t1: [FilterExec: DynamicFilter [ expression_id_0_hash_0 ]]}
          │       ProjectionExec: expr=[count(Int64(1))@1 as n]
          │         AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │           [Stage 3] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=12
            │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
            │   ProjectionExec: expr=[count(Int64(1))@1 as n]
            │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=3
              │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
              └──────────────────────────────────────────────────
            ┌───── Stage 3 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    /// TopK crosses a shuffle to the FilterExec beneath the ordered aggregate's sort.
    #[tokio::test]
    async fn remote_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT "MinTemp", ARRAY_AGG("MaxTemp" ORDER BY "MaxTemp") AS temperatures
                FROM weather
                GROUP BY "MinTemp"
                ORDER BY "MinTemp"
                LIMIT 1
            "#,
        )
        .expect_dynamic_filter_updates()
        .execute()
        .await?;
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST], fetch=1
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=3
          │ ProjectionExec: expr=[MinTemp@0 as MinTemp, array_agg(weather.MaxTemp) ORDER BY [weather.MaxTemp ASC NULLS LAST]@1 as temperatures]
          │   SortExec: TopK(fetch=1), expr=[MinTemp@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[MinTemp@0 as MinTemp], aggr=[array_agg(weather.MaxTemp) ORDER BY [weather.MaxTemp ASC NULLS LAST]]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([MinTemp@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[MinTemp@0 as MinTemp], aggr=[array_agg(weather.MaxTemp) ORDER BY [weather.MaxTemp ASC NULLS LAST]]
            │     SortExec: expr=[MaxTemp@1 ASC NULLS LAST], preserve_partitioning=[true]
            │       FilterExec: task_variants={t0: [FilterExec: DynamicFilter [ expression_id_0_hash_0 ]], t1: [FilterExec: DynamicFilter [ expression_id_0_hash_0 ]]}
            │         DistributedLeafExec:
            │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, MaxTemp], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            │           t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, MaxTemp], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }
}
