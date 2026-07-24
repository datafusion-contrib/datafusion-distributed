#[cfg(test)]
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
}
