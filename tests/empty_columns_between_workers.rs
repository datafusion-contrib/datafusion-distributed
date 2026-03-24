#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DefaultSessionBuilder, assert_snapshot, display_plan_ascii};
    use futures::TryStreamExt;
    use std::error::Error;

    /// Reproducer for "must either specify a row count or at least one column" error.
    ///
    /// When a query projects only literals (e.g. `SELECT 1 FROM t WHERE ...`),
    /// the intermediate stages produce record batches with zero columns. Arrow's
    /// IPC format rejects these when they are sent between workers.
    #[tokio::test]
    async fn empty_columns_between_workers() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        // Scalar subqueries produce all output columns, so the main table scan
        // (GROUP BY "RainToday") ends up with a ProjectionExec: expr=[] — zero
        // columns — that gets sent through NetworkCoalesceExec. Arrow IPC rejects
        // zero-column batches with "must either specify a row count or at least
        // one column".
        let query = r#"
            SELECT
                CASE WHEN (SELECT count(*) FROM weather WHERE "RainToday" = 'Yes') > 10
                     THEN (SELECT avg("Rainfall") FROM weather WHERE "RainToday" = 'Yes')
                     ELSE (SELECT avg("Rainfall") FROM weather WHERE "RainToday" = 'No')
                END as result
            FROM weather
            GROUP BY "RainToday"
            LIMIT 1
        "#;

        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = display_plan_ascii(physical.as_ref(), false);

        // The plan shows ProjectionExec: expr=[] in Stage 2, which produces
        // zero-column batches sent over the network.
        assert_snapshot!(physical_str, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   ProjectionExec: expr=[CASE WHEN count(*)@0 > 10 THEN avg(weather.Rainfall)@1 ELSE avg(weather.Rainfall)@2 END as result]
        │     RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
        │       GlobalLimitExec: skip=0, fetch=1
        │         NestedLoopJoinExec: join_type=Left
        │           NestedLoopJoinExec: join_type=Left
        │             NestedLoopJoinExec: join_type=Left
        │               CoalescePartitionsExec: fetch=1
        │                 [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        │               ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │                 AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │                   CoalescePartitionsExec
        │                     [Stage 3] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        │             AggregateExec: mode=Final, gby=[], aggr=[avg(weather.Rainfall)]
        │               CoalescePartitionsExec
        │                 [Stage 4] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        │           AggregateExec: mode=Final, gby=[], aggr=[avg(weather.Rainfall)]
        │             CoalescePartitionsExec
        │               [Stage 5] => NetworkCoalesceExec: output_partitions=3, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ ProjectionExec: expr=[]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[], lim=[1]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=1
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[], lim=[1]
            │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
            │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0] t1:[p1] t2:[p2] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   ProjectionExec: expr=[]
          │     FilterExec: RainToday@0 = Yes
          │       PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet, predicate=RainToday@19 = Yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= Yes AND Yes <= RainToday_max@1, required_guarantees=[RainToday in (Yes)]
          └──────────────────────────────────────────────────
          ┌───── Stage 4 ── Tasks: t0:[p0] t1:[p1] t2:[p2] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(weather.Rainfall)]
          │   FilterExec: RainToday@1 = Yes, projection=[Rainfall@0]
          │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=RainToday@19 = Yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= Yes AND Yes <= RainToday_max@1, required_guarantees=[RainToday in (Yes)]
          └──────────────────────────────────────────────────
          ┌───── Stage 5 ── Tasks: t0:[p0] t1:[p1] t2:[p2] 
          │ AggregateExec: mode=Partial, gby=[], aggr=[avg(weather.Rainfall)]
          │   FilterExec: RainToday@1 = No, projection=[Rainfall@0]
          │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0]
          │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=RainToday@19 = No, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= No AND No <= RainToday_max@1, required_guarantees=[RainToday in (No)]
          └──────────────────────────────────────────────────
        ");

        let batches = execute_stream(physical, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        let formatted = pretty_format_batches(&batches)?;
        assert_snapshot!(formatted, @"");

        Ok(())
    }
}
