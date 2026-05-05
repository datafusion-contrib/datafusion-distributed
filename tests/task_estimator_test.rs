#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::pretty_format_batches;
    use datafusion::{error::DataFusionError, execution::SessionState, physical_plan::collect};
    use datafusion_distributed::{
        DistributedExt, WorkerQueryContext, assert_snapshot, display_plan_ascii,
        test_utils::{
            in_memory_channel_resolver::start_in_memory_context,
            routing::{URLEmitterExtensionCodec, URLEmitterFunction, URLEmitterTaskEstimator},
        },
    };

    #[tokio::test]
    async fn custom_routing() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(5, 5, 'logs')
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
          │     URLEmitterExec: tasks=5 partitions=5 tag=logs
          └──────────────────────────────────────────────────
        +------------+------------+------+--------------+
        | task_count | task_index | tag  | worker_url   |
        +------------+------------+------+--------------+
        | 5          | 0          | logs | http://url-4 |
        | 5          | 1          | logs | http://url-3 |
        | 5          | 2          | logs | http://url-2 |
        | 5          | 3          | logs | http://url-1 |
        | 5          | 4          | logs | http://url-0 |
        +------------+------------+------+--------------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_routing_more_partitions() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(8, 5, 'logs')
            ORDER BY task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @"
        ┌───── DistributedExec ── Tasks: t0:[p0]
        │ SortPreservingMergeExec: [task_index@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=10, input_tasks=5
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] t2:[p4..p5] t3:[p6..p7] t4:[p8..p9]
          │ SortExec: expr=[task_index@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   PartitionIsolatorExec: tasks=5 partitions=8
          │     URLEmitterExec: tasks=5 partitions=8 tag=logs
          └──────────────────────────────────────────────────
        +------------+------------+------+--------------+
        | task_count | task_index | tag  | worker_url   |
        +------------+------------+------+--------------+
        | 5          | 0          | logs | http://url-4 |
        | 5          | 0          | logs | http://url-4 |
        | 5          | 1          | logs | http://url-3 |
        | 5          | 1          | logs | http://url-3 |
        | 5          | 2          | logs | http://url-2 |
        | 5          | 2          | logs | http://url-2 |
        | 5          | 3          | logs | http://url-1 |
        | 5          | 4          | logs | http://url-0 |
        +------------+------------+------+--------------+
        ",
        );
        Ok(())
    }

    #[tokio::test]
    async fn custom_routing_more_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(3, 5, 'logs')
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
          │   PartitionIsolatorExec: tasks=5 partitions=3
          │     URLEmitterExec: tasks=5 partitions=3 tag=logs
          └──────────────────────────────────────────────────
        +------------+------------+------+--------------+
        | task_count | task_index | tag  | worker_url   |
        +------------+------------+------+--------------+
        | 5          | 0          | logs | http://url-4 |
        | 5          | 1          | logs | http://url-3 |
        | 5          | 2          | logs | http://url-2 |
        +------------+------------+------+--------------+
        ",
        );
        Ok(())
    }

    async fn run_query(sql: &str) -> Result<(String, String), DataFusionError> {
        let mut ctx = start_in_memory_context(5, build_state).await;
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
