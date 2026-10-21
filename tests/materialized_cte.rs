#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int32Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::{HashMap, Result};
    use datafusion::config::Dialect;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::MappedWorkerSessionBuilderExt;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, display_plan_ascii,
    };
    use datafusion_distributed::{TaskKey, TaskMetrics};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use test_case::test_case;
    use tokio::time::timeout;

    #[test_case(false; "static_placement")]
    #[test_case(true; "adaptive_config")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn references_replay_the_same_volatile_result(adaptive: bool) -> Result<()> {
        let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx
            .with_distributed_dynamic_task_count(adaptive)?
            .with_distributed_metrics_collection(true)?;
        configure(&ctx);
        let path = register_source(&ctx, 1024).await?;
        let plan = ctx
            .sql(
                "WITH x AS MATERIALIZED (SELECT id, random() AS v FROM source) \
            SELECT count(*) AS rows, count(*) FILTER (WHERE a.v = b.v) AS equal_values \
            FROM x a JOIN x b ON a.id = b.id",
            )
            .await?
            .create_physical_plan()
            .await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        assert!(display.contains("NetworkMaterializedScanExec"), "{display}");
        assert!(display.contains("NetworkShuffleExec"), "{display}");
        let batches = collect_result(&ctx, &plan, path).await?;
        assert_eq!(
            pretty_format_batches(&batches)?.to_string(),
            "+------+--------------+\n| rows | equal_values |\n+------+--------------+\n| 1024 | 1024         |\n+------+--------------+"
        );
        let metrics = wait_for_metrics(&plan).await;
        let buffered_rows: usize = metrics
            .values()
            .flat_map(|task| &task.pre_order_plan_metrics)
            .filter_map(|metrics| metrics.sum_by_name("buffered_rows"))
            .map(|value| value.as_usize())
            .sum();
        assert_eq!(
            buffered_rows, 1024,
            "the producer must execute once across all references"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spilled_result_can_be_replayed_by_both_references() -> Result<()> {
        let builder = DefaultSessionBuilder.map(|builder| {
            Ok(builder
                .with_runtime_env(
                    RuntimeEnvBuilder::new()
                        .with_memory_limit(2 * 1024 * 1024, 1.0)
                        .build_arc()?,
                )
                .build())
        });
        let (ctx, _workers, _) = start_localhost_context(2, builder).await;
        let ctx = ctx.with_distributed_metrics_collection(true)?;
        configure(&ctx);
        let path = register_source(&ctx, 200_000).await?;
        let plan = ctx
            .sql(
                "WITH x AS MATERIALIZED (SELECT id, random() AS v FROM source) \
            SELECT max(v) FROM x UNION ALL SELECT max(v) FROM x",
            )
            .await?
            .create_physical_plan()
            .await?;
        let batches = collect_result(&ctx, &plan, path).await?;
        let values: Vec<_> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], values[1]);
        let metrics = wait_for_metrics(&plan).await;
        let spilled_rows: usize = metrics
            .values()
            .flat_map(|task| &task.pre_order_plan_metrics)
            .filter_map(|metrics| metrics.spilled_rows())
            .sum();
        assert!(spilled_rows > 0, "the shared producer must actually spill");
        Ok(())
    }

    #[test_case("WITH x AS MATERIALIZED (SELECT random() AS v WHERE false) SELECT count(*) = 0 AS same_value FROM x a, x b"; "empty_result")]
    #[test_case("WITH x AS MATERIALIZED (SELECT id, random() AS v FROM (VALUES (1), (2)) AS source(id)) SELECT (SELECT count(*) FROM x WHERE id = 1) = 1 AND (SELECT count(*) FROM x WHERE id = 2) = 1 AS same_value"; "independent_reader_predicates")]
    #[test_case("WITH x AS MATERIALIZED (SELECT random() AS v), y AS MATERIALIZED (SELECT v FROM x) SELECT a.v = b.v AND b.v = c.v AS same_value FROM x a, y b, y c"; "nested_producers")]
    #[test_case("WITH x AS MATERIALIZED (SELECT random() AS v) SELECT v = (SELECT max(v) FROM x) AS same_value FROM x"; "scalar_reference")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shared_references_preserve_sql_semantics(sql: &str) -> Result<()> {
        let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        configure(&ctx);
        let batches = timeout(Duration::from_secs(10), async {
            ctx.sql(sql).await?.collect().await
        })
        .await
        .expect("dependent readers must complete")?;
        assert_eq!(
            pretty_format_batches(&batches)?.to_string(),
            "+------------+\n| same_value |\n+------------+\n| true       |\n+------------+"
        );
        Ok(())
    }

    fn configure(ctx: &SessionContext) {
        let state = ctx.state_ref();
        let mut state = state.write();
        let options = state.config_mut().options_mut();
        options.sql_parser.dialect = Dialect::PostgreSQL;
        options.execution.target_partitions = 4;
        // Make the probe reader wait for the build reader to finish.
        options.execution.hash_join_buffering_capacity = 0;
        options.optimizer.hash_join_single_partition_threshold = 0;
        options.optimizer.hash_join_single_partition_threshold_rows = 0;
    }

    async fn collect_result(
        ctx: &SessionContext,
        plan: &Arc<dyn ExecutionPlan>,
        source: PathBuf,
    ) -> Result<Vec<RecordBatch>> {
        let result = timeout(
            Duration::from_secs(30),
            collect(Arc::clone(plan), ctx.task_ctx()),
        )
        .await;
        fs::remove_file(source)?;
        result.expect("materialized readers must not deadlock")
    }

    async fn register_source(ctx: &SessionContext, rows: i32) -> Result<PathBuf> {
        let batch = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int32Array::from_iter_values(0..rows)) as _,
        )])?;
        register_temp_parquet_table("source", batch.schema(), vec![batch], ctx).await
    }

    async fn wait_for_metrics(plan: &Arc<dyn ExecutionPlan>) -> HashMap<TaskKey, TaskMetrics> {
        timeout(
            Duration::from_secs(10),
            plan.downcast_ref::<DistributedExec>()
                .unwrap()
                .wait_for_metrics(),
        )
        .await
        .expect("materialization tasks must finish")
        .unwrap()
    }
}
