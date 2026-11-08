#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::common::assert_contains;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DefaultSessionBuilder, display_plan_ascii};
    use futures::TryStreamExt;
    use std::error::Error;

    /// Reproducer for "must either specify a row count or at least one column" error.
    ///
    /// When a query projects only literals (e.g. `SELECT 1 FROM t WHERE ...`),
    /// the intermediate stages produce record batches with zero columns. Arrow's
    /// IPC format rejects these when they are sent between workers.
    #[tokio::test]
    async fn empty_columns_between_workers() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        let query = r#"
            SELECT (SELECT count(*) FROM weather) FROM weather GROUP BY "RainToday"
        "#;

        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = display_plan_ascii(physical.as_ref(), false);

        // The plan should be distributed
        assert_contains!(physical_str, "DistributedExec");

        // Executes without failing.
        execute_stream(physical, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }
}
