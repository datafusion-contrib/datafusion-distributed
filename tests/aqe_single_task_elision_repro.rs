#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::assert_batches_sorted_eq;
    use datafusion::common::Result;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, display_plan_ascii,
    };
    use std::sync::Arc;

    #[tokio::test]
    async fn static_small_query_stays_local() -> Result<()> {
        let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx
            .with_distributed_file_scan_config_bytes_per_partition(128 * 1024 * 1024)?
            .with_distributed_dynamic_bytes_per_partition(128 * 1024 * 1024)?
            .with_distributed_dynamic_task_count(false)?;
        register_parquet_tables(&ctx).await?;
        let plan = ctx
            .sql(r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#)
            .await?
            .create_physical_plan()
            .await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(
            [
                "+-----------+----------+",
                "| RainToday | count(*) |",
                "+-----------+----------+",
                "| No        | 300      |",
                "| Yes       | 66       |",
                "+-----------+----------+",
            ],
            &batches
        );
        assert!(!plan.is::<DistributedExec>());
        Ok(())
    }

    #[tokio::test]
    async fn aqe_small_query_should_not_open_worker_channels() -> Result<()> {
        let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx
            .with_distributed_file_scan_config_bytes_per_partition(128 * 1024 * 1024)?
            .with_distributed_dynamic_bytes_per_partition(128 * 1024 * 1024)?
            .with_distributed_dynamic_task_count(true)?;
        register_parquet_tables(&ctx).await?;
        let plan = ctx
            .sql(r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#)
            .await?
            .create_physical_plan()
            .await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(
            [
                "+-----------+----------+",
                "| RainToday | count(*) |",
                "+-----------+----------+",
                "| No        | 300      |",
                "| Yes       | 66       |",
                "+-----------+----------+",
            ],
            &batches
        );
        assert_eq!(
            coordinator_channels(plan.as_ref()),
            0,
            "AQE retained unnecessary single-task stages:\n{}",
            display_plan_ascii(plan.as_ref(), false)
        );
        assert!(!plan.is::<DistributedExec>());
        Ok(())
    }

    #[tokio::test]
    async fn aqe_still_adapts_distributed_queries() -> Result<()> {
        let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx
            .with_distributed_file_scan_config_bytes_per_partition(1)?
            .with_distributed_dynamic_bytes_per_partition(128 * 1024 * 1024)?
            .with_distributed_dynamic_task_count(true)?;
        register_parquet_tables(&ctx).await?;
        let plan = ctx
            .sql(r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#)
            .await?
            .create_physical_plan()
            .await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(
            [
                "+-----------+----------+",
                "| RainToday | count(*) |",
                "+-----------+----------+",
                "| No        | 300      |",
                "| Yes       | 66       |",
                "+-----------+----------+",
            ],
            &batches
        );
        // Two scan tasks, then one final-aggregation task chosen from the sample.
        // Static planning would use two tasks for each stage instead.
        assert!(plan.is::<DistributedExec>());
        assert_eq!(coordinator_channels(plan.as_ref()), 3);
        Ok(())
    }

    #[tokio::test]
    async fn aqe_compute_budget_can_distribute_a_small_scan() -> Result<()> {
        let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx
            .with_distributed_file_scan_config_bytes_per_partition(128 * 1024 * 1024)?
            .with_distributed_dynamic_bytes_per_partition(1)?
            .with_distributed_dynamic_task_count(true)?;
        register_parquet_tables(&ctx).await?;
        let plan = ctx
            .sql(r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#)
            .await?
            .create_physical_plan()
            .await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(
            [
                "+-----------+----------+",
                "| RainToday | count(*) |",
                "+-----------+----------+",
                "| No        | 300      |",
                "| Yes       | 66       |",
                "+-----------+----------+",
            ],
            &batches
        );
        assert!(plan.is::<DistributedExec>());
        assert_eq!(coordinator_channels(plan.as_ref()), 4);
        Ok(())
    }

    #[tokio::test]
    async fn aqe_keeps_sampling_when_scan_statistics_are_unknown() -> Result<()> {
        let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        let ctx = ctx
            .with_distributed_file_scan_config_bytes_per_partition(128 * 1024 * 1024)?
            .with_distributed_dynamic_bytes_per_partition(128 * 1024 * 1024)?
            .with_distributed_dynamic_task_count(true)?;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .collect_statistics = false;
        register_parquet_tables(&ctx).await?;
        let plan = ctx
            .sql(r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#)
            .await?
            .create_physical_plan()
            .await?;
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_batches_sorted_eq!(
            [
                "+-----------+----------+",
                "| RainToday | count(*) |",
                "+-----------+----------+",
                "| No        | 300      |",
                "| Yes       | 66       |",
                "+-----------+----------+",
            ],
            &batches
        );
        assert!(plan.is::<DistributedExec>());
        assert_eq!(coordinator_channels(plan.as_ref()), 2);
        Ok(())
    }

    fn coordinator_channels(plan: &dyn ExecutionPlan) -> usize {
        plan.metrics()
            .and_then(|metrics| {
                metrics.sum(|metric| metric.value().name().ends_with("coordinator_channels"))
            })
            .map_or(0, |metric| metric.as_usize())
    }
}
