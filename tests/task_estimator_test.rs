#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use arrow::util::pretty::pretty_format_batches;
    use datafusion::{error::DataFusionError, physical_plan::collect};
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, assert_snapshot, display_plan_ascii,
        test_utils::{
            localhost::start_localhost_context,
            routing::{URLEmitterFunction, URLEmitterTaskEstimator},
        },
    };

    #[tokio::test]
    async fn custom_task_estimator_with_routing() -> Result<(), Box<dyn std::error::Error>> {
        let (plan, results) = run_query(
            r#"
            SELECT task_count, task_index, worker_url
            FROM url_emitter()
        "#,
        )
        .await?;

        assert_snapshot!(plan + &results,
            @"
        CooperativeExec
          URLEmitterExec: [   TODO   ]
        +------------+------------+------------+
        | task_count | task_index | worker_url |
        +------------+------------+------------+
        +------------+------------+------------+
        ",
        );
        Ok(())
    }

    async fn run_query(sql: &str) -> Result<(String, String), DataFusionError> {
        let (mut ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        ctx.set_distributed_task_estimator(URLEmitterTaskEstimator);
        ctx.register_udtf("url_emitter", Arc::new(URLEmitterFunction));

        let df = ctx.sql(sql).await?;
        let plan = df.create_physical_plan().await?;
        let plan_display = display_plan_ascii(plan.as_ref(), false);

        let batches = collect(plan, ctx.task_ctx()).await?;
        let formatted = pretty_format_batches(&batches)?;

        Ok((plan_display, formatted.to_string()))
    }
}
