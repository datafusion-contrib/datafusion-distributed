#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::{ExecutionPlan, execute_stream};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DefaultSessionBuilder, assert_snapshot, display_plan_ascii};
    use futures::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn more_tasks_than_children() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard) = start_localhost_context(3, DefaultSessionBuilder).await;

        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        ORDER BY "MinTemp", "RainToday"
        "#;

        let ctx = SessionContext::default();
        *ctx.state_ref().write().config_mut() = ctx_distributed.copied_config();
        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;

        register_parquet_tables(&ctx_distributed).await?;
        ctx_distributed
            .sql("SET distributed.children_isolator_unions=true;")
            .await?;
        let df_distributed = ctx_distributed.sql(query).await?;
        let physical_distributed = df_distributed.create_physical_plan().await?;
        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8] 
          │ DistributedUnionExec: t0:[c0] t1:[c1(0/2)] t2:[c1(1/2)]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: MinTemp@0 > 10
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: MaxTemp@0 < 30
          │           PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
          │             DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@0 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          └──────────────────────────────────────────────────
        ",
        );

        exact_same_data(ctx.task_ctx(), physical, physical_distributed).await
    }

    #[tokio::test]
    async fn same_children_than_tasks() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard) = start_localhost_context(3, DefaultSessionBuilder).await;

        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 20.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 25.0
        UNION ALL
        SELECT "Temp9am", "RainToday" FROM weather WHERE "Temp9am" > 15.0
        ORDER BY "MinTemp", "RainToday"
        "#;

        let ctx = SessionContext::default();
        *ctx.state_ref().write().config_mut() = ctx_distributed.copied_config();
        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;

        register_parquet_tables(&ctx_distributed).await?;
        ctx_distributed
            .sql("SET distributed.children_isolator_unions=true;")
            .await?;
        let df_distributed = ctx_distributed.sql(query).await?;
        let physical_distributed = df_distributed.create_physical_plan().await?;
        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p2] t1:[p3..p5] t2:[p6..p8] 
          │ DistributedUnionExec: t0:[c0] t1:[c1] t2:[c2]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: MinTemp@0 > 20
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 20, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 20, required_guarantees=[]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: MaxTemp@0 < 25
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@0 < 25, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 25, required_guarantees=[]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[Temp9am@0 as MinTemp, RainToday@1 as RainToday]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: Temp9am@0 > 15
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@0 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
          └──────────────────────────────────────────────────
        ",
        );

        exact_same_data(ctx.task_ctx(), physical, physical_distributed).await
    }

    #[tokio::test]
    async fn more_children_than_tasks() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard) = start_localhost_context(3, DefaultSessionBuilder).await;

        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        UNION ALL
        SELECT "Temp9am", "RainToday" FROM weather WHERE "Temp9am" > 15.0
        UNION ALL
        SELECT "Temp3pm", "RainToday" FROM weather WHERE "Temp3pm" < 25.0
        UNION ALL
        SELECT "Rainfall", "RainToday" FROM weather WHERE "Rainfall" > 5.0
        ORDER BY "MinTemp", "RainToday"
        "#;

        let ctx = SessionContext::default();
        *ctx.state_ref().write().config_mut() = ctx_distributed.copied_config();
        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;

        register_parquet_tables(&ctx_distributed).await?;
        ctx_distributed
            .sql("SET distributed.children_isolator_unions=true;")
            .await?;
        let df_distributed = ctx_distributed.sql(query).await?;
        let physical_distributed = df_distributed.create_physical_plan().await?;
        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=18, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p6..p11] t2:[p12..p17] 
          │ DistributedUnionExec: t0:[c0, c1] t1:[c2, c3] t2:[c4]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: MinTemp@0 > 10
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[MaxTemp@0 as MinTemp, RainToday@1 as RainToday]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: MaxTemp@0 < 30
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@0 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[Temp9am@0 as MinTemp, RainToday@1 as RainToday]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: Temp9am@0 > 15
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@0 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[Temp3pm@0 as MinTemp, RainToday@1 as RainToday]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: Temp3pm@0 < 25
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp3pm, RainToday], file_type=parquet, predicate=Temp3pm@0 < 25, pruning_predicate=Temp3pm_null_count@1 != row_count@2 AND Temp3pm_min@0 < 25, required_guarantees=[]
          │   SortExec: expr=[MinTemp@0 ASC NULLS LAST, RainToday@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     ProjectionExec: expr=[Rainfall@0 as MinTemp, RainToday@1 as RainToday]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         FilterExec: Rainfall@0 > 5
          │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=Rainfall@0 > 5, pruning_predicate=Rainfall_null_count@1 != row_count@2 AND Rainfall_max@0 > 5, required_guarantees=[]
          └──────────────────────────────────────────────────
        ",
        );

        exact_same_data(ctx.task_ctx(), physical, physical_distributed).await
    }

    async fn exact_same_data(
        task_ctx: Arc<TaskContext>,
        one: Arc<dyn ExecutionPlan>,
        other: Arc<dyn ExecutionPlan>,
    ) -> Result<(), Box<dyn Error>> {
        let batches = pretty_format_batches(
            &execute_stream(one, task_ctx.clone())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        let batches_distributed = pretty_format_batches(
            &execute_stream(other, task_ctx)?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        // Verify that both plans produce the same results
        assert_eq!(batches.to_string(), batches_distributed.to_string());
        Ok(())
    }
}
