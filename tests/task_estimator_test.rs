#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::pretty_format_batches;
    use datafusion::{
        catalog::memory::DataSourceExec, datasource::physical_plan::FileScanConfig,
        physical_plan::ExecutionPlan, prelude::ParquetReadOptions,
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedConfig, DistributedExt, NetworkBoundary,
        NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, PartitionIsolatorExec,
        PlannedLeafNode, TaskEstimation, TaskEstimator, assert_snapshot,
        test_utils::localhost::start_localhost_context,
    };
    use futures::TryStreamExt;
    use url::Url;

    #[tokio::test]
    async fn custom_task_estimator_with_routing() -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            SELECT "RainToday", count(*)
            FROM weather
            WHERE "Sunshine" > 5 AND "Rainfall" > 0
            GROUP BY "RainToday"
            ORDER BY "RainToday";
        "#;

        let (d_ctx, _guard, _) = start_localhost_context(5, DefaultSessionBuilder).await;
        let d_ctx = d_ctx.with_distributed_task_estimator(CustomTaskEstimator);
        d_ctx
            .register_parquet("weather", "testdata/weather", ParquetReadOptions::default())
            .await?;

        let df = d_ctx.sql(query).await?;
        let plan = df.clone().create_physical_plan().await?;

        let session_config = d_ctx.copied_config();
        let urls = DistributedConfig::from_config_options(session_config.options())?.get_urls()?;
        let stage = bottom_input_stage(plan.as_ref()).expect("expected a network boundary");
        let routed_urls = custom_routing_fn(urls.clone(), stage.tasks.len());
        assert_ne!(routed_urls, urls[..stage.tasks.len()]);

        // Assert that the stage was correctly formed, with tasks attached according to the
        // user-defined routing function.
        for (task, routed_url) in stage.tasks.iter().zip(routed_urls.iter()) {
            assert_eq!(task.url.as_ref(), Some(routed_url));
        }

        // From here, the query is executed normally, with tasks routed to the specified URLs.
        let stream = df.execute_stream().await?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        let output = pretty_format_batches(&batches)?;
        assert_snapshot!(output, @"
        +-----------+----------+
        | RainToday | count(*) |
        +-----------+----------+
        | No        | 15       |
        | Yes       | 33       |
        +-----------+----------+
        ");

        Ok(())
    }

    #[derive(Clone)]
    pub struct CustomTaskEstimator;

    impl TaskEstimator for CustomTaskEstimator {
        fn task_estimation(
            &self,
            plan: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            _cfg: &datafusion::config::ConfigOptions,
        ) -> datafusion::error::Result<Option<TaskEstimation>> {
            if let Some(exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
                let file_scan_cfg = exec
                    .data_source()
                    .as_any()
                    .downcast_ref::<FileScanConfig>()
                    .unwrap();
                let num_file_groups = file_scan_cfg.file_groups.len();
                Ok(Some(TaskEstimation::desired(num_file_groups)))
            } else {
                Ok(None)
            }
        }

        fn plan_leaf_node(
            &self,
            plan: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
            task_count: usize,
            cfg: &datafusion::config::ConfigOptions,
        ) -> datafusion::error::Result<Option<datafusion_distributed::PlannedLeafNode>> {
            let plan: Arc<dyn ExecutionPlan> =
                Arc::new(PartitionIsolatorExec::new(Arc::clone(plan), task_count));
            let mut planned_leaf_node = PlannedLeafNode::from_plan(&plan);

            // Add some custom routing.
            let urls = DistributedConfig::from_config_options(cfg)?.get_urls()?;
            let routed_urls = custom_routing_fn(urls, task_count);
            planned_leaf_node.with_urls(Some(routed_urls), task_count)?;

            Ok(Some(planned_leaf_node))
        }
    }

    fn custom_routing_fn(mut urls: Vec<Url>, task_count: usize) -> Vec<Url> {
        // Trivial routing policy.
        urls.reverse();
        urls.truncate(task_count);
        urls
    }

    // Routing tasks to URLs only occurs at leaves. This test helper function is used to
    // inspect whether URLs were attached to the stage governing the leaves.
    fn bottom_input_stage(plan: &dyn ExecutionPlan) -> Option<datafusion_distributed::Stage> {
        for child in plan.children() {
            if let Some(stage) = bottom_input_stage(child.as_ref()) {
                return Some(stage);
            }
        }
        if let Some(plan) = plan.as_any().downcast_ref::<NetworkShuffleExec>() {
            Some(plan.input_stage().clone())
        } else if let Some(plan) = plan.as_any().downcast_ref::<NetworkCoalesceExec>() {
            Some(plan.input_stage().clone())
        } else {
            plan.as_any()
                .downcast_ref::<NetworkBroadcastExec>()
                .map(|plan| plan.input_stage().clone())
        }
    }
}
