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
