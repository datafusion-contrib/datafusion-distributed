#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::internal_err;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::physical_plan::collect;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, DistributedMetricsFormat, NetworkBoundaryExt,
        display_plan_ascii, rewrite_distributed_plan_with_metrics,
    };
    use std::sync::Arc;

    /// During dynamic planning, if the planner decides that all the stages in the query are small
    /// enough to fit in a single machine, it should co-locate them on in the machine that contains
    /// the coordinating context, avoiding all network jumps.
    #[tokio::test]
    async fn all_local_connections_dynamic_planner() -> Result<(), Box<dyn std::error::Error>> {
        let (mut ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await?;
        ctx.set_distributed_dynamic_task_count(true)?;
        ctx.set_distributed_file_scan_config_bytes_per_partition(1024 * 1024)?;

        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;

        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;
        collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        let format = DistributedMetricsFormat::Aggregated;
        let plan = rewrite_distributed_plan_with_metrics(plan, format).await?;
        println!("{}", display_plan_ascii(plan.as_ref(), true));
        plan.apply(|plan| {
            if !plan.is_network_boundary() {
                return Ok(TreeNodeRecursion::Continue);
            };

            let metrics = plan.metrics().unwrap();
            let local_connections_used = metrics
                .sum(|v| v.value().name() == "local_connections_used")
                .map_or(0, |v| v.as_usize());
            if local_connections_used == 0 {
                return internal_err!("local_connections_used==0");
            };

            Ok(TreeNodeRecursion::Continue)
        })?;

        Ok(())
    }

    #[tokio::test]
    async fn half_coordinator_connections_are_local() -> Result<(), Box<dyn std::error::Error>> {
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;

        let plan = ctx.sql(query).await?.create_physical_plan().await?;
        collect(Arc::clone(&plan), ctx.task_ctx()).await?;

        let metrics = plan.metrics().expect("DistributedExec has metrics");
        let metric_value = |name| {
            metrics
                .sum(|metric| metric.value().name() == name)
                .map(|metric| metric.as_usize())
                .unwrap_or_default()
        };

        // The plan above has two stages, with two tasks each, so 4 tasks in total. Assuming the
        // coordinator is the worker 0, the coordinator->worker channels are the following:
        // - coordinator worker 0 -> stage 0, worker 0 | local
        // - coordinator worker 0 -> stage 0, worker 1 | remote
        // - coordinator worker 0 -> stage 1, worker 0 | local
        // - coordinator worker 0 -> stage 1, worker 1 | remote
        assert_eq!(metric_value("local_coordinator_channels"), 2);
        assert_eq!(metric_value("remote_coordinator_channels"), 2);

        Ok(())
    }

    #[tokio::test]
    async fn network_boundaries_use_local_connections() -> Result<(), Box<dyn std::error::Error>> {
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;

        let plan = ctx.sql(query).await?.create_physical_plan().await?;
        collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        let plan =
            rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::Aggregated)
                .await?;

        let mut local_connections_used = 0;
        plan.apply(|node| {
            if node.is_network_boundary() {
                local_connections_used += node
                    .metrics()
                    .unwrap_or_default()
                    .sum(|metric| metric.value().name() == "local_connections_used")
                    .map_or(0, |metric| metric.as_usize());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;

        // The plan above has two stages, with two tasks each, so 4 tasks in total. Assuming the
        // coordinator is the worker 0, the network boundary->worker channels are the following:
        // - coordinator worker 0 -> stage 1, worker 0 | local
        // - coordinator worker 0 -> stage 1, worker 1 | remote
        // - stage 1, worker 0 -> stage 0, worker 0    | local
        // - stage 1, worker 0 -> stage 0, worker 1    | remote
        // - stage 1, worker 1 -> stage 0, worker 0    | remote
        // - stage 1, worker 1 -> stage 0, worker 1    | local
        assert_eq!(local_connections_used, 3);

        Ok(())
    }
}
