#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::exec_datafusion_err;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DistributedExt, WorkerQueryContext, display_plan_ascii};
    use futures::TryStreamExt;
    use http::{HeaderMap, HeaderName, HeaderValue};

    async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
        Ok(ctx
            .builder
            // by erroring here if the header is not present, we make sure that the header gets
            // propagated across stages. If the header was not propagated across stages, it would
            // error right here before even forming a SessionState inside a Worker.
            .with_distributed_passthrough_headers(HeaderMap::from_iter([(
                HeaderName::from_static("foo"),
                ctx.headers
                    .get("foo")
                    .cloned()
                    .ok_or_else(|| exec_datafusion_err!("Missing header foo"))?,
            )]))?
            .build())
    }

    #[tokio::test]
    async fn custom_header() -> Result<(), Box<dyn std::error::Error>> {
        let (mut ctx, _guard, _) = start_localhost_context(3, build_state).await;
        ctx.set_distributed_passthrough_headers(HeaderMap::from_iter([(
            HeaderName::from_static("foo"),
            HeaderValue::from_static("bar"),
        )]))?;

        let query = r#"SELECT DISTINCT "RainToday", "WindGustDir" FROM weather"#;

        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let plan = df.create_physical_plan().await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        println!("{display}");

        let stream = execute_stream(plan, ctx.task_ctx())?;
        // It should not fail.
        stream.try_collect::<Vec<_>>().await?;

        Ok(())
    }
}
