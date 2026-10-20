#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use datafusion::common::{Result, Statistics};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion::prelude::SessionConfig;
    use datafusion_distributed::{DistributedExt, WorkUnitFeedProvider};
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;
    use datafusion_distributed_iceberg::{IcebergConfig, IcebergDataSource, IcebergWorkUnitFeed};
    use futures::TryStreamExt;
    use prost::Message;

    #[tokio::test]
    async fn byte_balanced_assignment_preserves_distributed_results() -> Result<()> {
        let balanced = IcebergTestHarness::builder()
            .with_workers(3)
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = true;
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
                .contains("requires iceberg.eager_file_planning=true")
        );
        Ok(())
    }

    #[tokio::test]
    async fn eager_and_lazy_scans_return_the_same_rows_and_statistics() -> Result<()> {
        let lazy = harness(false, false).await?;
        let eager = harness(true, false).await?;
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
        let lazy = harness(false, true).await?;
        let eager = harness(true, true).await?;
        let query = "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
                     WHERE pickup_date <= DATE '2024-01-11' \
                     GROUP BY pickup_date ORDER BY pickup_date";
        assert_eq!(lazy.query(query).await?.1, eager.query(query).await?.1);
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
                config.eager_file_planning = true;
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

    #[tokio::test]
    async fn both_modes_claim_feeds_once_and_clones_get_fresh_execution() -> Result<()> {
        for (eager, greedy) in [(false, false), (true, false), (true, true)] {
            let harness = IcebergTestHarness::builder()
                .configure_session(|mut state| {
                    let mut config = IcebergConfig::default();
                    config.eager_file_planning = eager;
                    config.greedy_file_assignment = greedy;
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
            let scan = harness.scan().await?;
            let partitions = scan.properties().partitioning.partition_count();
            let provider = scan
                .downcast_ref::<DataSourceExec>()
                .unwrap()
                .data_source()
                .downcast_ref::<IcebergDataSource>()
                .unwrap()
                .feed()
                .inner()
                .unwrap();
            let first = read_feed(provider, partitions, greedy).await?;
            assert_eq!(first.len(), 7);
            assert!(provider.feed(0, Arc::new(TaskContext::default())).is_err());
            assert_eq!(
                read_feed(&provider.clone(), partitions, greedy).await?,
                first
            );
        }
        Ok(())
    }

    async fn read_feed(
        provider: &IcebergWorkUnitFeed,
        partitions: usize,
        greedy: bool,
    ) -> Result<Vec<Vec<u8>>> {
        let mut config = SessionConfig::new();
        let mut iceberg = IcebergConfig::default();
        iceberg.greedy_file_assignment = greedy;
        config.options_mut().extensions.insert(iceberg);
        let context = Arc::new(TaskContext::default().with_session_config(config));
        let mut files = vec![];
        for partition in 0..partitions {
            let units: Vec<_> = provider
                .feed(partition, Arc::clone(&context))?
                .try_collect()
                .await?;
            files.extend(units.into_iter().map(|unit| unit.encode_to_vec()));
        }
        files.sort();
        Ok(files)
    }

    async fn statistics(harness: &IcebergTestHarness) -> Result<Arc<Statistics>> {
        let scan = harness.scan().await?;
        StatisticsContext::new().compute(scan.as_ref(), &StatisticsArgs::new())
    }

    async fn harness(eager: bool, workers: bool) -> Result<IcebergTestHarness> {
        let builder = IcebergTestHarness::builder();
        let builder = if workers {
            builder.with_workers(2)
        } else {
            builder
        };
        builder
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = eager;
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
