#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::error::Result;
    use datafusion::execution::SessionState;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{DistributedExt, WorkerQueryContext, display_plan_ascii};
    use datafusion_distributed_iceberg::test_utils::{FIXTURE_URI, FixtureStorageFactory};
    use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};
    use futures::TryStreamExt;

    #[tokio::test]
    async fn reads_iceberg_work_units_on_remote_workers() -> Result<()> {
        let (mut ctx, _guard, _) = start_localhost_context(2, build_worker_state).await;
        ctx.set_iceberg_integration(integration_options());
        // #603 must exercise plan serialization without relying on #605's Iceberg task estimator.
        ctx.set_distributed_desired_task_count_handler(2usize);
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
             LOCATION '{FIXTURE_URI}/metadata/v1.metadata.json'"
        ))
        .await?
        .collect()
        .await?;

        let dataframe = ctx
            .sql(
                "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
                 GROUP BY pickup_date ORDER BY pickup_date",
            )
            .await?;
        let plan = dataframe.create_physical_plan().await?;
        let batches = execute_stream(Arc::clone(&plan), ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let plan = display_plan_ascii(plan.as_ref(), false);
        let remote_stage_headers = plan
            .lines()
            .filter(|line| line.contains(" Stage "))
            .collect::<Vec<_>>();
        assert_eq!(
            remote_stage_headers,
            [
                "  ┌───── Stage 2 ── tasks=2, partitions=3",
                "    ┌───── Stage 1 ── tasks=2, partitions=6",
            ]
        );

        let results = pretty_format_batches(&batches)?.to_string();
        assert_eq!(
            results,
            "+-------------+-------+\n\
             | pickup_date | trips |\n\
             +-------------+-------+\n\
             | 2024-01-08  | 25000 |\n\
             | 2024-01-09  | 25000 |\n\
             | 2024-01-10  | 25000 |\n\
             | 2024-01-11  | 25000 |\n\
             | 2024-01-12  | 25000 |\n\
             | 2024-01-13  | 25000 |\n\
             | 2024-01-14  | 25000 |\n\
             +-------------+-------+"
        );
        Ok(())
    }

    async fn build_worker_state(ctx: WorkerQueryContext) -> Result<SessionState> {
        Ok(ctx
            .builder
            .with_iceberg_integration(integration_options())
            .build())
    }

    fn integration_options() -> IcebergIntegrationOptions {
        IcebergIntegrationOptions {
            storage_factory: Arc::new(FixtureStorageFactory::default()),
            iceberg_runtime: datafusion_distributed_iceberg::iceberg::Runtime::current(),
        }
    }
}
