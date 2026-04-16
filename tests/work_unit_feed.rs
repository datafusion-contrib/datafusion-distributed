#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::test_work_unit_feed::{
        TestWorkUnitFeedExecCodec, TestWorkUnitFeedFunction, TestWorkUnitFeedTaskEstimator,
    };
    use datafusion_distributed::{
        DistributedExt, WorkerQueryContext, assert_snapshot, display_plan_ascii,
    };
    use futures::TryStreamExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn single_task_no_distribution() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit(1, '1,1', '2')
            ORDER BY task, partition, string
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        SortPreservingMergeExec: [task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST]
          SortExec: expr=[task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST], preserve_partitioning=[true]
            WorkUnitFeedExec: RowGeneratorFeedProvider: tasks=1, rows_per_partition=[[1, 1], [2]]
              RowGeneratorExec
        +------+-----------+--------+
        | task | partition | string |
        +------+-----------+--------+
        | 0    | 0         | a      |
        | 0    | 0         | a      |
        | 0    | 1         | a      |
        | 0    | 1         | b      |
        +------+-----------+--------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn two_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit(2, '1,1', '2', '1', '2,1')
            ORDER BY task, partition, string
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] 
          │ SortExec: expr=[task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST], preserve_partitioning=[true]
          │   WorkUnitFeedExec: RowGeneratorFeedProvider: tasks=2, rows_per_partition=[[1, 1], [2], [1], [2, 1]]
          │     RowGeneratorExec
          └──────────────────────────────────────────────────
        +------+-----------+--------+
        | task | partition | string |
        +------+-----------+--------+
        | 0    | 0         | a      |
        | 0    | 0         | a      |
        | 0    | 1         | a      |
        | 0    | 1         | b      |
        | 1    | 0         | a      |
        | 1    | 1         | a      |
        | 1    | 1         | a      |
        | 1    | 1         | b      |
        +------+-----------+--------+
        ",
        );
        Ok(())
    }

    /// Tests that empty work unit feeds (no work units) produce no rows for that partition,
    /// while other partitions still work correctly through the distributed path.
    #[tokio::test]
    async fn empty_work_unit_feeds() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit(2, '3', '', '', '1')
            ORDER BY task, partition, string
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] 
          │ SortExec: expr=[task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST], preserve_partitioning=[true]
          │   WorkUnitFeedExec: RowGeneratorFeedProvider: tasks=2, rows_per_partition=[[3], [], [], [1]]
          │     RowGeneratorExec
          └──────────────────────────────────────────────────
        +------+-----------+--------+
        | task | partition | string |
        +------+-----------+--------+
        | 0    | 0         | a      |
        | 0    | 0         | b      |
        | 0    | 0         | c      |
        | 1    | 1         | a      |
        +------+-----------+--------+
        ",
        );
        Ok(())
    }

    /// Tests distribution across three tasks.
    #[tokio::test]
    async fn three_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT * FROM test_work_unit(3, '2', '1', '3', '1', '2', '1')
            ORDER BY task, partition, string
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] 
          │ SortExec: expr=[task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST], preserve_partitioning=[true]
          │   WorkUnitFeedExec: RowGeneratorFeedProvider: tasks=3, rows_per_partition=[[2], [1], [3], [1], [2], [1]]
          │     RowGeneratorExec
          └──────────────────────────────────────────────────
        +------+-----------+--------+
        | task | partition | string |
        +------+-----------+--------+
        | 0    | 0         | a      |
        | 0    | 0         | b      |
        | 0    | 1         | a      |
        | 1    | 0         | a      |
        | 1    | 0         | b      |
        | 1    | 0         | c      |
        | 1    | 1         | a      |
        | 2    | 0         | a      |
        | 2    | 0         | b      |
        | 2    | 1         | a      |
        +------+-----------+--------+
        ",
        );
        Ok(())
    }

    async fn run_query(sql: &str) -> Result<(String, String), DataFusionError> {
        let (mut ctx, _guard, _) = start_localhost_context(3, build_state).await;
        ctx.set_distributed_user_codec(TestWorkUnitFeedExecCodec);
        ctx.set_distributed_task_estimator(TestWorkUnitFeedTaskEstimator);
        ctx.register_udtf("test_work_unit", Arc::new(TestWorkUnitFeedFunction));

        let df = ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        let plan_display = display_plan_ascii(plan.as_ref(), false);

        let batches = execute_stream(plan, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let formatted = pretty_format_batches(&batches)?;

        Ok((plan_display, formatted.to_string()))
    }

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_distributed_user_codec(TestWorkUnitFeedExecCodec)
            .build())
    }
}
