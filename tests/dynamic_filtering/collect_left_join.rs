#[cfg(test)]
mod tests {
    use crate::common::{
        LOCAL_AND_REMOTE_UNION_QUERY, TestQuery, normalize_runtime_dynamic_filters,
    };
    use datafusion::common::Result;
    use datafusion_distributed::assert_snapshot;

    /// A CollectLeft HashJoinExec producer propagates identical updates to local consumers.
    #[tokio::test]
    async fn local_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN weather probe ON build.key = probe."RainToday"
            "#,
        )
        .with_broadcast_joins()
        .execute()
        .await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=6
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     CoalescePartitionsExec
          │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=2
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ RainToday@19 >= No AND RainToday@19 <= Yes AND RainToday@19 IN (SET) ([<values>]) ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@1 != row_count@2 AND RainToday_max@0 >= No AND RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= Yes AND (RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= Yes AND Yes <= RainToday_max@0 OR RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= No AND No <= RainToday_max@0), required_guarantees=[RainToday in (No, Yes)]
          │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ RainToday@19 >= No AND RainToday@19 <= Yes AND RainToday@19 IN (SET) ([<values>]) ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@1 != row_count@2 AND RainToday_max@0 >= No AND RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= Yes AND (RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= Yes AND Yes <= RainToday_max@0 OR RainToday_null_count@1 != row_count@2 AND RainToday_min@3 <= No AND No <= RainToday_max@0), required_guarantees=[RainToday in (No, Yes)]
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=12
            │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
            │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
            │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
              │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    /// A CollectLeft HashJoinExec propagates its first complete filter to remote consumers.
    #[tokio::test]
    async fn remote_dynamic_filters() -> Result<()> {
        let display = normalize_runtime_dynamic_filters(
            TestQuery::new(
                r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                RIGHT SEMI JOIN (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) probe ON build.key = probe.key
            "#,
            )
            .with_broadcast_joins()
            .execute()
            .await?,
        );
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 4] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 4 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(key@0, key@0)], projection=[]
          │     CoalescePartitionsExec
          │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=2
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 3] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=12
            │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
            │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
            │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
              │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              └──────────────────────────────────────────────────
            ┌───── Stage 3 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn union_with_local_and_remote_probe_children() -> Result<()> {
        let display = normalize_runtime_dynamic_filters(
            TestQuery::new(LOCAL_AND_REMOTE_UNION_QUERY)
                .with_broadcast_joins()
                .execute()
                .await?,
        );
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 5] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 5 ── tasks=2, partitions=6
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(key@0, key@0)], projection=[]
          │     CoalescePartitionsExec
          │       [Stage 2] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=2
          │     DistributedUnionExec: t0:[c0] t1:[c1]
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
          │       ProjectionExec: expr=[WindGustDir@0 as key]
          │         HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(key@0, RainToday@1)], projection=[WindGustDir@0]
          │           CoalescePartitionsExec
          │             [Stage 4] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=3, input_tasks=2
          │           DistributedLeafExec:
          │             t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir, RainToday], file_type=parquet, predicate=DynamicFilter [ <runtime> ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=12
            │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
            │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
            │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
              │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
              └──────────────────────────────────────────────────
            ┌───── Stage 4 ── tasks=2, partitions=6
            │ BroadcastExec: input_partitions=3, consumer_tasks=1, output_partitions=3
            │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
            │     [Stage 3] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
              │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn union_probe_deduplicates_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "MinTemp" AS key
                    FROM weather
                ) build
                JOIN (
                    SELECT "MinTemp" FROM weather
                    UNION ALL
                    SELECT CAST(-1000.0 AS DOUBLE) AS "MinTemp"
                    UNION ALL
                    SELECT "MinTemp" FROM weather
                    UNION ALL
                    SELECT CAST(-1001.0 AS DOUBLE) AS "MinTemp"
                    UNION ALL
                    SELECT CAST(-1002.0 AS DOUBLE) AS "MinTemp"
                ) probe ON build.key = probe."MinTemp"
            "#,
        )
        .with_broadcast_joins()
        // Indirectly forces the union to put c0 and c2 on the same task. We would like to test
        // that consumers of a dynamic filter on the same node get the same filter.
        //
        // If we don't do this, inject_network_boundaries splits them up to spread out the load.
        .with_one_task_per_leaf()
        .execute()
        .await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 2] => NetworkCoalesceExec: output_partitions=14, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=14
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(key@0, MinTemp@0)], projection=[]
          │     CoalescePartitionsExec
          │       [Stage 1] => NetworkBroadcastExec: partitions_per_consumer=3, stage_partitions=6, input_tasks=1
          │     DistributedUnionExec: t0:[c0, c2, c4] t1:[c1, c3]
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ MinTemp@0 >= -5.3 AND MinTemp@0 <= 20.9 AND true ], dynamic_rg_pruning=eligible, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 >= -5.3 AND MinTemp_null_count@1 != row_count@2 AND MinTemp_min@3 <= 20.9, required_guarantees=[]
          │       ProjectionExec: expr=[CAST(-1000 AS Float64) as MinTemp]
          │         PlaceholderRowExec
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ MinTemp@0 >= -5.3 AND MinTemp@0 <= 20.9 AND true ], dynamic_rg_pruning=eligible, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 >= -5.3 AND MinTemp_null_count@1 != row_count@2 AND MinTemp_min@3 <= 20.9, required_guarantees=[]
          │       ProjectionExec: expr=[CAST(-1001 AS Float64) as MinTemp]
          │         PlaceholderRowExec
          │       ProjectionExec: expr=[CAST(-1002 AS Float64) as MinTemp]
          │         PlaceholderRowExec
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=1, partitions=6
            │ BroadcastExec: input_partitions=3, consumer_tasks=2, output_partitions=6
            │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
            │     RepartitionExec: partitioning=Hash([key@0], 3), input_partitions=3
            │       AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │         DistributedLeafExec:
            │           t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp@0 as key], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }
}
