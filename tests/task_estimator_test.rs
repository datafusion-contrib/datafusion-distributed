#[cfg(all(feature = "integration", test))]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow::array::{Int64Array, StringArray};
    use datafusion::{error::DataFusionError, execution::SessionState, physical_plan::collect};
    use datafusion_distributed::{
        DistributedExt, WorkerQueryContext, assert_snapshot, display_plan_ascii,
        get_distributed_worker_resolver,
        test_utils::{
            localhost::start_localhost_context,
            routing::{URLEmitterExtensionCodec, URLEmitterFunction, URLEmitterTaskEstimator},
        },
    };

    #[tokio::test]
    #[ignore = "modifying URL emitter"]
    async fn custom_routing() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, actual_routing, expected_routing) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(5, 5, 'logs')
            ORDER BY task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan,
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
        ",
        );

        assert_eq!(actual_routing, expected_routing);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "modifying URL emitter"]
    async fn custom_routing_more_partitions() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, actual_routing, expected_routing) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(8, 5, 'logs')
            ORDER BY task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan,
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
        ",
        );

        assert_eq!(actual_routing, expected_routing);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "modifying URL emitter"]
    async fn custom_routing_more_tasks() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, actual_routing, expected_routing) = run_query(
            r#"
            SELECT task_count, task_index, tag, worker_url
            FROM url_emitter(3, 5, 'logs')
            ORDER BY task_index
        "#,
        )
        .await?;

        assert_snapshot!(plan,
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
        ",
        );

        assert_eq!(actual_routing, expected_routing);
        Ok(())
    }

    async fn run_query(
        sql: &str,
    ) -> Result<(String, BTreeMap<i64, String>, BTreeMap<i64, String>), DataFusionError> {
        let (mut ctx, _guard, _) = start_localhost_context(5, build_state).await;
        let worker_urls = get_distributed_worker_resolver(ctx.state().config())?.get_urls()?;
        ctx.set_distributed_task_estimator(URLEmitterTaskEstimator);
        ctx.set_distributed_user_codec(URLEmitterExtensionCodec);
        ctx.register_udtf("url_emitter", Arc::new(URLEmitterFunction));

        let df = ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        let plan_display = display_plan_ascii(plan.as_ref(), false);

        let batches = collect(plan, ctx.task_ctx()).await?;

        // Extract the mapping of tasks to URLs observed following query execution. We cannot
        // simply snapshot the results, because the localhost URLs used may vary on each run.
        let mut actual_routing = BTreeMap::new();
        for batch in batches {
            let task_index = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("task_index column should be Int64Array");
            let worker_url = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("worker_url column should be StringArray");

            for row in 0..batch.num_rows() {
                let task_index = task_index.value(row);
                let worker_url = worker_url.value(row).to_string();
                if let Some(previous) = actual_routing.insert(task_index, worker_url.clone()) {
                    assert_eq!(
                        previous, worker_url,
                        "task_index {task_index} emitted conflicting worker URLs"
                    )
                }
            }
        }

        // Simulate the routing function defined above. We can use this simulated routing to observe
        // whether the mapping of tasks to URLs following query execution matches what we expect.
        let mut expected_routing = worker_urls.clone();
        expected_routing.reverse();
        expected_routing.truncate(actual_routing.len());

        Ok((
            plan_display,
            actual_routing,
            expected_routing
                .into_iter()
                .enumerate()
                .map(|(task_index, url)| (task_index as i64, url.to_string()))
                .collect(),
        ))
    }

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            .with_distributed_user_codec(URLEmitterExtensionCodec)
            .build())
    }
}
