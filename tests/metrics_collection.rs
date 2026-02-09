#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::catalog::memory::DataSourceExec;
    use datafusion::common::assert_not_contains;
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    use datafusion::physical_plan::display::DisplayableExecutionPlan;
    use datafusion::physical_plan::{ExecutionPlan, execute_stream};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, DistributedMetricsFormat, NetworkCoalesceExec,
        NetworkShuffleExec, display_plan_ascii, rewrite_distributed_plan_with_metrics,
    };
    use futures::TryStreamExt;
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(DistributedMetricsFormat::Aggregated ; "aggregated_metrics")]
    #[test_case(DistributedMetricsFormat::PerTask ; "per_task_metrics")]
    #[tokio::test]
    async fn test_metrics_collection_in_aggregation(
        format: DistributedMetricsFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut d_ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        d_ctx.set_distributed_bytes_processed_per_partition(1000)?;

        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;

        let s_ctx = SessionContext::default();
        let (s_physical, mut d_physical) = execute(&s_ctx, &d_ctx, query).await?;
        d_physical = rewrite_distributed_plan_with_metrics(d_physical.clone(), format)?;
        println!("{}", display_plan_ascii(s_physical.as_ref(), true));
        println!("{}", display_plan_ascii(d_physical.as_ref(), true));

        assert_metrics_equal::<DataSourceExec>(
            ["output_rows", "output_bytes"],
            &s_physical,
            &d_physical,
            0,
        );

        Ok(())
    }

    #[test_case(DistributedMetricsFormat::Aggregated ; "aggregated_metrics")]
    #[test_case(DistributedMetricsFormat::PerTask ; "per_task_metrics")]
    #[tokio::test]
    async fn test_metrics_collection_in_join(
        format: DistributedMetricsFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut d_ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        d_ctx.set_distributed_bytes_processed_per_partition(1000)?;

        let query = r#"
        WITH a AS (
            SELECT
                AVG("MinTemp") as "MinTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'Yes'
            GROUP BY "RainTomorrow"
        ), b AS (
            SELECT
                AVG("MaxTemp") as "MaxTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'No'
            GROUP BY "RainTomorrow"
        )
        SELECT
            a."MinTemp",
            b."MaxTemp"
        FROM a
        LEFT JOIN b
        ON a."RainTomorrow" = b."RainTomorrow"
        "#;

        let s_ctx = SessionContext::default();
        let (s_physical, mut d_physical) = execute(&s_ctx, &d_ctx, query).await?;
        d_physical = rewrite_distributed_plan_with_metrics(d_physical.clone(), format)?;
        println!("{}", display_plan_ascii(s_physical.as_ref(), true));
        println!("{}", display_plan_ascii(d_physical.as_ref(), true));

        for data_source_index in 0..2 {
            assert_metrics_equal::<DataSourceExec>(
                ["output_rows", "output_bytes"],
                &s_physical,
                &d_physical,
                data_source_index,
            );
        }

        Ok(())
    }

    #[test_case(DistributedMetricsFormat::Aggregated ; "aggregated_metrics")]
    #[test_case(DistributedMetricsFormat::PerTask ; "per_task_metrics")]
    #[tokio::test]
    async fn test_metrics_collection_in_union(
        format: DistributedMetricsFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut d_ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        d_ctx.set_distributed_bytes_processed_per_partition(1000)?;

        let query = r#"
        SELECT "MinTemp", "RainToday" FROM weather WHERE "MinTemp" > 10.0
        UNION ALL
        SELECT "MaxTemp", "RainToday" FROM weather WHERE "MaxTemp" < 30.0
        UNION ALL
        SELECT "Temp9am", "RainToday" FROM weather WHERE "Temp9am" > 15.0
        UNION ALL
        SELECT "Temp3pm", "RainToday" FROM weather WHERE "Temp3pm" < 25.0
        UNION ALL
        SELECT "Rainfall", "RainToday" FROM weather WHERE "Rainfall" > 5.0
        "#;

        let s_ctx = SessionContext::default();
        let (s_physical, mut d_physical) = execute(&s_ctx, &d_ctx, query).await?;

        d_physical = rewrite_distributed_plan_with_metrics(d_physical.clone(), format)?;
        println!("{}", display_plan_ascii(s_physical.as_ref(), true));
        println!("{}", display_plan_ascii(d_physical.as_ref(), true));

        for data_source_index in 0..5 {
            assert_metrics_equal::<DataSourceExec>(
                ["output_rows", "output_bytes"],
                &s_physical,
                &d_physical,
                data_source_index,
            );
        }
        Ok(())
    }

    #[test_case(DistributedMetricsFormat::Aggregated ; "aggregated_metrics")]
    #[test_case(DistributedMetricsFormat::PerTask ; "per_task_metrics")]
    #[tokio::test]
    async fn test_metric_collection_network_boundaries(
        format: DistributedMetricsFormat,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut d_ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        d_ctx.set_distributed_bytes_processed_per_partition(1000)?;

        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;

        let s_ctx = SessionContext::default();
        let (s_physical, mut d_physical) = execute(&s_ctx, &d_ctx, query).await?;
        d_physical = rewrite_distributed_plan_with_metrics(d_physical.clone(), format)?;
        println!("{}", display_plan_ascii(s_physical.as_ref(), true));
        println!("{}", display_plan_ascii(d_physical.as_ref(), true));

        let value = node_metrics::<NetworkCoalesceExec>(&d_physical, "bytes_transferred", 1);
        assert!(value > 100);
        let value = node_metrics::<NetworkCoalesceExec>(&d_physical, "max_mem_used", 1);
        assert!(value > 100);
        let value = node_metrics::<NetworkCoalesceExec>(&d_physical, "elapsed_compute", 1);
        assert!(value > 100);
        let value = node_metrics::<NetworkCoalesceExec>(&d_physical, "network_latency_avg", 1);
        assert!(value > 0);

        let value = node_metrics::<NetworkShuffleExec>(&d_physical, "bytes_transferred", 1);
        assert!(value > 100);
        let value = node_metrics::<NetworkShuffleExec>(&d_physical, "max_mem_used", 1);
        assert!(value > 100);
        let value = node_metrics::<NetworkShuffleExec>(&d_physical, "elapsed_compute", 1);
        assert!(value > 100);
        let value = node_metrics::<NetworkShuffleExec>(&d_physical, "network_latency_avg", 1);
        assert!(value > 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_metric_collection_display_all_have_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let format = DistributedMetricsFormat::PerTask;
        let (d_ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;

        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;

        let s_ctx = SessionContext::default();
        let (_, mut d_physical) = execute(&s_ctx, &d_ctx, query).await?;
        d_physical = rewrite_distributed_plan_with_metrics(d_physical.clone(), format)?;

        let display =
            DisplayableExecutionPlan::with_metrics(d_physical.children().swap_remove(0).as_ref())
                .indent(true)
                .to_string();
        assert_not_contains!(display, "metrics=[]");

        let display = display_plan_ascii(d_physical.as_ref(), true);
        assert_not_contains!(display, "metrics=[]");

        Ok(())
    }

    /// Looks for an [ExecutionPlan] that matches the provided type parameter `T` in
    /// both root nodes and compares its metrics.
    /// There might be more than one, so `index` determines which one is compared.
    ///
    /// If the two root nodes contain a child T with different metrics, the assertion fails.
    fn assert_metrics_equal<T: ExecutionPlan + 'static>(
        names: impl IntoIterator<Item = &'static str>,
        one: &Arc<dyn ExecutionPlan>,
        other: &Arc<dyn ExecutionPlan>,
        index: usize,
    ) {
        for name in names.into_iter() {
            let one_metric = node_metrics::<T>(one, name, index);
            let other_metric = node_metrics::<T>(other, name, index);
            assert_eq!(one_metric, other_metric);
        }
    }

    async fn execute(
        s_ctx: &SessionContext,
        d_ctx: &SessionContext,
        query: &str,
    ) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>), Box<dyn std::error::Error>> {
        register_parquet_tables(s_ctx).await?;
        register_parquet_tables(d_ctx).await?;

        let s_df = s_ctx.sql(query).await?;
        let s_physical = s_df.create_physical_plan().await?;
        execute_stream(s_physical.clone(), s_ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        let d_df = d_ctx.sql(query).await?;
        let d_physical = d_df.create_physical_plan().await?;
        execute_stream(d_physical.clone(), d_ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        Ok((s_physical, d_physical))
    }

    fn node_metrics<T: ExecutionPlan + 'static>(
        plan: &Arc<dyn ExecutionPlan>,
        metric_name: &str,
        mut index: usize,
    ) -> usize {
        let mut metrics = None;
        plan.clone()
            .transform_down(|plan| {
                if plan.name() == T::static_name() {
                    metrics = plan.metrics();
                    if index == 0 {
                        return Ok(Transformed::new(plan, false, TreeNodeRecursion::Stop));
                    }
                    index -= 1;
                }
                Ok(Transformed::no(plan))
            })
            .unwrap();
        let metrics = metrics
            .unwrap_or_else(|| panic!("Could not find metrics for plan {}", T::static_name()));
        let summed = metrics
            .iter()
            .filter(|v| v.value().name().starts_with(metric_name))
            .map(|v| v.value().as_usize())
            .sum();
        assert!(
            summed > 0,
            "Sum of metric values is 0. Either the metric {metric_name} is not present or the test is too trivial"
        );
        summed
    }
}
