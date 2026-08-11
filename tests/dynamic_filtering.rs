#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::Result;
    use datafusion::physical_plan::collect;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DefaultSessionBuilder, DistributedExt, display_plan_ascii};
    use std::sync::Arc;

    #[tokio::test]
    async fn completed_leaf_dynamic_filters_are_displayed_per_task() -> Result<()> {
        for dynamic_task_count in [false, true] {
            assert_completed_leaf_filters(dynamic_task_count).await?;
        }
        Ok(())
    }

    async fn assert_completed_leaf_filters(dynamic_task_count: bool) -> Result<()> {
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx.with_distributed_dynamic_task_count(dynamic_task_count)?;
        register_parquet_tables(&ctx).await?;

        let plan = ctx
            .sql(
                r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN weather probe ON build.key = probe."RainToday"
                "#,
            )
            .await?
            .create_physical_plan()
            .await?;

        let results = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_eq!(
            results.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );

        let display = display_plan_ascii(plan.as_ref(), false);
        let task_filters: Vec<_> = display
            .lines()
            .filter(|line| line.contains(": DataSourceExec:"))
            .filter_map(|line| line.split_once("DynamicFilter [ "))
            .map(|(_, filter)| filter.split_once(" ]").unwrap().0)
            .collect();

        assert!(
            task_filters.len() >= 2,
            "expected task-local leaf filters in plan:\n{display}"
        );
        assert!(
            task_filters.iter().all(|filter| *filter != "empty"),
            "expected completed filters in plan:\n{display}"
        );
        Ok(())
    }
}
