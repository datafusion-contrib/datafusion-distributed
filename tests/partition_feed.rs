#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::partition_feed::{
        TestPartitionFeedExecCodec, TestPartitionFeedFunction, TestPartitionFeedTaskEstimator,
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
            SELECT * FROM test_partition_feed(1, '1,1', '2')
            ORDER BY task, partition, string
        "#,
        )
        .await?;

        assert_snapshot!(plan,
            @r"
        SortPreservingMergeExec: [task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST]
          SortExec: expr=[task@0 ASC NULLS LAST, partition@1 ASC NULLS LAST, string@2 ASC NULLS LAST], preserve_partitioning=[true]
            CooperativeExec
              PartitionFeedExec: feeds=2
                TestPartitionFeedExec: tasks=1, rows_per_partition=[[1, 1], [2]]
        ",
        );

        assert_snapshot!(results,
            @r"
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

    async fn run_query(sql: &str) -> Result<(String, String), DataFusionError> {
        let (mut ctx, _guard, _) = start_localhost_context(3, build_state).await;
        ctx.set_distributed_user_codec(TestPartitionFeedExecCodec);
        ctx.set_distributed_task_estimator(TestPartitionFeedTaskEstimator);
        ctx.register_udtf("test_partition_feed", Arc::new(TestPartitionFeedFunction));

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
            .with_distributed_user_codec(TestPartitionFeedExecCodec)
            .build())
    }
}
