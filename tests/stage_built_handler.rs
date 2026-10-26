#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::{Result, assert_contains, exec_err};
    use datafusion::physical_plan::coop::CooperativeExec;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, StageBuiltEvent, StageBuiltEventResponse,
        StageBuiltHandler, TaskCountAnnotation, assert_snapshot, display_plan_ascii,
    };
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use test_case::test_case;

    const NUM_WORKERS: usize = 4;
    const QUERY: &str =
        r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;
    const EXPECTED: &str = "\
+----------+-----------+
| count(*) | RainToday |
+----------+-----------+
| 66       | Yes       |
| 300      | No        |
+----------+-----------+";

    #[tokio::test]
    async fn exact_task_count_static_plan() -> Result<()> {
        let (plan, results) = run_query(false, exact_task_count_handler(2)).await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=3
          │ ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          │   SortExec: expr=[count(Int64(1))@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
        assert_eq!(results, EXPECTED);
        Ok(())
    }

    #[test_case(false, 2; "static_task_count_2_tasks")]
    #[test_case(false, 3; "static_task_count_3_tasks")]
    #[test_case(false, 4; "static_task_count_4_tasks")]
    #[test_case(true, 2; "dynamic_task_count_2_tasks")]
    #[test_case(true, 3; "dynamic_task_count_3_tasks")]
    #[test_case(true, 4; "dynamic_task_count_4_tasks")]
    #[tokio::test]
    async fn exact_task_count(dynamic_task_count: bool, tasks: usize) -> Result<()> {
        let (plan, results) =
            run_query(dynamic_task_count, exact_task_count_handler(tasks)).await?;
        assert_contains!(&plan, format!("Stage 1 ── tasks={tasks}"));
        assert_contains!(&plan, format!("Stage 2 ── tasks={tasks}"));
        assert_eq!(results, EXPECTED);
        Ok(())
    }

    #[test_case(false; "static_task_count")]
    #[test_case(true; "dynamic_task_count")]
    #[tokio::test]
    async fn exact_task_count_above_max_tasks_fails(dynamic_task_count: bool) -> Result<()> {
        let err = run_query(
            dynamic_task_count,
            exact_task_count_handler(NUM_WORKERS * 2),
        )
        .await
        .expect_err("an exact task count above the max tasks should fail");
        assert_contains!(
            err.to_string(),
            "Cannot assign a required task count of 8, the max task slots available are 4"
        );
        Ok(())
    }

    #[test_case(false; "static_task_count")]
    #[test_case(true; "dynamic_task_count")]
    #[tokio::test]
    async fn soft_task_count_is_capped_by_max_tasks(dynamic_task_count: bool) -> Result<()> {
        let (plan, results) = run_query(dynamic_task_count, |ev: StageBuiltEvent<'_>| {
            Ok(StageBuiltEventResponse::new(ev.plan)
                .with_task_count(TaskCountAnnotation::soft(f64::MAX)))
        })
        .await?;
        assert_contains!(&plan, format!("Stage 1 ── tasks={NUM_WORKERS}"));
        assert_eq!(results, EXPECTED);
        Ok(())
    }

    #[test_case(false; "static_task_count")]
    #[test_case(true; "dynamic_task_count")]
    #[tokio::test]
    async fn rewrite_below_stage_head(dynamic_task_count: bool) -> Result<()> {
        let (plan, results) = run_query(dynamic_task_count, |ev: StageBuiltEvent<'_>| {
            let Some(r_exec) = ev.plan.downcast_ref::<RepartitionExec>() else {
                return Ok(StageBuiltEventResponse::new(ev.plan));
            };
            let input = Arc::new(CooperativeExec::new(Arc::clone(r_exec.input())));
            let plan = ev.plan.replace_children(
                vec![input],
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?;
            Ok(StageBuiltEventResponse::new(plan))
        })
        .await?;
        assert_contains!(&plan, "CooperativeExec");
        assert_eq!(results, EXPECTED);
        Ok(())
    }

    #[test_case(false; "static_task_count")]
    #[test_case(true; "dynamic_task_count")]
    #[tokio::test]
    async fn handler_errors_propagate_to_query(dynamic_task_count: bool) -> Result<()> {
        let err = run_query(dynamic_task_count, |_: StageBuiltEvent<'_>| {
            exec_err!("stage rejected by handler")
        })
        .await
        .expect_err("handler error should propagate to the query");
        assert_contains!(err.to_string(), "stage rejected by handler");
        Ok(())
    }

    fn exact_task_count_handler(tasks: usize) -> impl StageBuiltHandler {
        move |ev: StageBuiltEvent<'_>| {
            Ok(
                StageBuiltEventResponse::new(ev.plan).with_task_count(TaskCountAnnotation::exact(
                    NonZeroUsize::new(tasks).unwrap(),
                    0.0,
                )),
            )
        }
    }

    async fn run_query(
        dynamic_task_count: bool,
        handler: impl StageBuiltHandler,
    ) -> Result<(String, String)> {
        let mut ctx: SessionContext = start_in_memory_context(NUM_WORKERS, DefaultSessionBuilder)
            .await
            .with_distributed_file_scan_config_bytes_per_partition(1)?
            .with_distributed_dynamic_bytes_per_partition(1)?;
        ctx.set_distributed_dynamic_task_count(dynamic_task_count)?;
        ctx.sql("SET datafusion.execution.target_partitions = 3")
            .await?;
        ctx.set_distributed_stage_built_handler(handler);
        register_parquet_tables(&ctx).await?;

        let plan = ctx.sql(QUERY).await?.create_physical_plan().await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        Ok((
            display_plan_ascii(plan.as_ref(), false),
            pretty_format_batches(&batches)?.to_string(),
        ))
    }
}
