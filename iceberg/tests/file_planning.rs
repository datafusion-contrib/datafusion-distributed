#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use datafusion::common::{Result, Statistics};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion_distributed_iceberg::IcebergConfig;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;

    #[tokio::test]
    async fn eager_and_lazy_scans_return_the_same_rows_and_statistics() -> Result<()> {
        let lazy = harness(false, 100_000, false).await?;
        let eager = harness(true, 100_000, false).await?;
        let query = "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
                     WHERE pickup_date IN (DATE '2024-01-10', DATE '2024-01-11') \
                     GROUP BY pickup_date ORDER BY pickup_date";

        assert_eq!(lazy.query(query).await?.1, eager.query(query).await?.1);
        assert_eq!(
            lazy.query("SELECT COUNT(*) FROM taxi WHERE pickup_date = DATE '1900-01-01'")
                .await?
                .1,
            eager
                .query("SELECT COUNT(*) FROM taxi WHERE pickup_date = DATE '1900-01-01'")
                .await?
                .1
        );
        let lazy_stats = statistics(&lazy).await?;
        let eager_stats = statistics(&eager).await?;
        assert_eq!(lazy_stats.num_rows, eager_stats.num_rows);
        assert_eq!(lazy_stats.total_byte_size, eager_stats.total_byte_size);
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_reuses_files_on_distributed_workers() -> Result<()> {
        let lazy = harness(false, 100_000, true).await?;
        let eager = harness(true, 100_000, true).await?;
        let query = "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
                     WHERE pickup_date <= DATE '2024-01-11' \
                     GROUP BY pickup_date ORDER BY pickup_date";
        assert_eq!(lazy.query(query).await?.1, eager.query(query).await?.1);
        Ok(())
    }

    #[tokio::test]
    async fn too_many_files_fails_during_planning_only_when_enabled() -> Result<()> {
        harness(false, 2, false)
            .await?
            .physical_plan("SELECT * FROM taxi")
            .await?;
        let error = harness(true, 2, false)
            .await?
            .physical_plan("SELECT * FROM taxi")
            .await
            .expect_err("the fixture has more than two files");
        assert!(
            error.to_string().contains("planning_max_files=2"),
            "{error}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn retained_files_respect_the_query_memory_pool() -> Result<()> {
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(1)))
                .build()?,
        );
        let eager = IcebergTestHarness::builder()
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.plan_files = true;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                Ok(state.with_runtime_env(runtime))
            })?
            .build()
            .await?;
        let error = eager
            .physical_plan("SELECT * FROM taxi")
            .await
            .expect_err("retained file tasks cannot fit in one byte");
        assert!(
            error.to_string().contains("Iceberg file planning"),
            "{error}"
        );
        Ok(())
    }

    async fn statistics(harness: &IcebergTestHarness) -> Result<Arc<Statistics>> {
        let scan = harness.scan().await?;
        StatisticsContext::new().compute(scan.as_ref(), &StatisticsArgs::new())
    }

    async fn harness(eager: bool, max_files: usize, workers: bool) -> Result<IcebergTestHarness> {
        let builder = IcebergTestHarness::builder();
        let builder = if workers {
            builder.with_workers(2)
        } else {
            builder
        };
        builder
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.plan_files = eager;
                config.planning_max_files = max_files;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                Ok(state)
            })?
            .build()
            .await
    }
}
