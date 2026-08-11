#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::Result;
    use datafusion::physical_plan::collect;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, assert_snapshot, display_plan_ascii,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn collect_left_local_dynamic_filters() -> Result<()> {
        let display = execute_local_hash_join(true).await?;
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

    #[tokio::test]
    async fn partitioned_local_dynamic_filters() -> Result<()> {
        let display = execute_local_hash_join(false).await?;
        assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=3
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    async fn execute_local_hash_join(broadcast_joins: bool) -> Result<String> {
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx.with_distributed_broadcast_joins(broadcast_joins)?;
        if !broadcast_joins {
            let state = ctx.state_ref();
            let mut state = state.write();
            let optimizer = &mut state.config_mut().options_mut().optimizer;
            optimizer.hash_join_single_partition_threshold = 0;
            optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        register_parquet_tables(&ctx).await?;

        let plan = ctx
            .sql(
                r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN weather probe ON build.key = probe."RainToday"
                "#,
            )
            .await?
            .create_physical_plan()
            .await?;

        let results = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_eq!(
            results.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );

        Ok(display_plan_ascii(plan.as_ref(), false))
    }
}
