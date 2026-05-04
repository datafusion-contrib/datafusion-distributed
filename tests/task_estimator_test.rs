#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::pretty_format_batches;
    use datafusion::{error::DataFusionError, execution::SessionState, physical_plan::collect};
    use datafusion_distributed::{
        DistributedExt, WorkerQueryContext, assert_snapshot, display_plan_ascii,
        test_utils::{
            localhost::start_localhost_context,
            routing::{URLEmitterExtensionCodec, URLEmitterFunction, URLEmitterTaskEstimator},
        },
    };

    #[tokio::test]
    async fn custom_task_estimator_with_routing() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, worker_url
            FROM url_emitter()
            ORDER BY task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [task_index@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=5, input_tasks=5
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0] t1:[p1] t2:[p2] t3:[p3] t4:[p4]
          │ SortExec: expr=[task_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   PartitionIsolatorExec: tasks=5 partitions=5
          │     URLEmitterExec: tasks=5 partitions=5
          └──────────────────────────────────────────────────
        +------------+------------+-------------+
        | task_count | task_index | worker_url  |
        +------------+------------+-------------+
        | 5          | 0          | example_url |
        | 5          | 1          | example_url |
        | 5          | 2          | example_url |
        | 5          | 3          | example_url |
        | 5          | 4          | example_url |
        +------------+------------+-------------+
        ",
        );
        Ok(())
    }

    async fn run_query(sql: &str) -> Result<(String, String), DataFusionError> {
        let (mut ctx, _guard, _) = start_localhost_context(5, build_state).await;
        ctx.set_distributed_task_estimator(URLEmitterTaskEstimator);
        ctx.set_distributed_user_codec(URLEmitterExtensionCodec);
        ctx.register_udtf("url_emitter", Arc::new(URLEmitterFunction));

        let df = ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        let plan_display = display_plan_ascii(plan.as_ref(), false);

        let batches = collect(plan, ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&batches)?;

        Ok((plan_display, formatted.to_string()))
    }

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_distributed_user_codec(URLEmitterExtensionCodec)
            .build())
    }
}
