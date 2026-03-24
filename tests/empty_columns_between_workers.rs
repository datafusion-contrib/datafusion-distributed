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

        // Scalar subqueries produce all output columns, so the main table scan
        // (GROUP BY "RainToday") ends up with a ProjectionExec: expr=[] — zero
        // columns — that gets sent through NetworkCoalesceExec. Arrow IPC rejects
        // zero-column batches with "must either specify a row count or at least
        // one column".
        let query = r#"
            SELECT
                CASE WHEN (SELECT count(*) FROM weather WHERE "RainToday" = 'Yes') > 10
                     THEN (SELECT avg("Rainfall") FROM weather WHERE "RainToday" = 'Yes')
                     ELSE (SELECT avg("Rainfall") FROM weather WHERE "RainToday" = 'No')
                END as result
            FROM weather
            GROUP BY "RainToday"
            LIMIT 1
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
