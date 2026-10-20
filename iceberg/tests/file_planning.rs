#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use datafusion::common::{Result, Statistics};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::{DistributedExt, WorkUnitFeedProvider};
    use datafusion_distributed_iceberg::IcebergConfig;
    use datafusion_distributed_iceberg::IcebergDataSource;
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
    async fn byte_balanced_assignment_preserves_distributed_results() -> Result<()> {
        let balanced = IcebergTestHarness::builder()
            .with_workers(3)
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.plan_files = true;
                config.greedy_file_assignment = true;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                state.with_distributed_file_scan_config_bytes_per_partition(1)
            })?
            .build()
            .await?;
        let query = "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
                     WHERE pickup_date <= DATE '2024-01-11' \
                     GROUP BY pickup_date ORDER BY pickup_date";
        let (_, results) = balanced.query(query).await?;
        let (_, local) = IcebergTestHarness::new().await?.query(query).await?;
        assert_eq!(results, local);
        Ok(())
    }

    #[tokio::test]
    async fn byte_balancing_requires_preplanned_files() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.greedy_file_assignment = true;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                Ok(state)
            })?
            .build()
            .await?;
        let error = harness
            .physical_plan("SELECT * FROM taxi")
            .await
            .expect_err("file assignment needs planning-time file discovery");
        assert!(
            error
                .to_string()
                .contains("requires iceberg.plan_files=true")
        );
        Ok(())
    }

    #[tokio::test]
    async fn greedy_assignment_charges_scratch_and_reuses_retained_groups() -> Result<()> {
        const CAPACITY: usize = 1024 * 1024;
        const HEADROOM: usize = 192;
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(CAPACITY));
        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build()?,
        );
        let mut iceberg = IcebergConfig::default();
        iceberg.plan_files = true;
        iceberg.greedy_file_assignment = true;
        let mut config = SessionConfig::new();
        config.options_mut().extensions.insert(iceberg);
        let harness = IcebergTestHarness::builder()
            .configure_session(|state| {
                Ok(state
                    .with_config(config.clone())
                    .with_runtime_env(Arc::clone(&runtime)))
            })?
            .build()
            .await?;
        let plan = harness.scan().await?;
        let discovered = pool.reserved();
        assert!(discovered > 0);
        let source = plan
            .downcast_ref::<DataSourceExec>()
            .expect("Iceberg source");
        let provider = source
            .data_source()
            .downcast_ref::<IcebergDataSource>()
            .expect("Iceberg data source")
            .feed()
            .inner()
            .expect("planned feed");
        let ctx = Arc::new(
            TaskContext::default()
                .with_session_config(config)
                .with_runtime(runtime),
        );
        let occupied = MemoryConsumer::new("test headroom").register(&pool);
        occupied.try_grow(CAPACITY - discovered - HEADROOM)?;
        let before = pool.reserved();
        let Err(error) = provider.feed(0, Arc::clone(&ctx)) else {
            panic!("assignment scratch must also fit");
        };
        assert!(
            error
                .to_string()
                .contains("Iceberg file assignment scratch"),
            "{error}"
        );
        assert_eq!(pool.reserved(), before, "failure must release reservations");

        drop(occupied);
        drop(provider.feed(0, Arc::clone(&ctx))?);
        let retained = pool.reserved();
        assert!(retained > discovered);
        assert!(
            retained - discovered <= HEADROOM,
            "scratch must be released"
        );
        drop(provider.feed(0, ctx)?);
        assert_eq!(pool.reserved(), retained, "assignment must be reused");
        drop(plan);
        assert_eq!(pool.reserved(), 0);
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
