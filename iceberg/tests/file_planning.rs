#[cfg(all(feature = "integration", test))]
mod tests {
    use std::sync::Arc;

    use datafusion::common::Result;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::collect;
    use datafusion_distributed::WorkUnitFeedProvider;
    use datafusion_distributed_iceberg::test_utils::IcebergTestHarness;
    use datafusion_distributed_iceberg::{IcebergWorkUnitFeed, LazyIcebergDataSource};
    use futures::TryStreamExt;
    use prost::Message;

    #[tokio::test]
    async fn eager_and_lazy_scans_return_the_same_rows() -> Result<()> {
        let lazy = IcebergTestHarness::builder()
            .with_column_stats_enabled()
            .build()
            .await?;
        let eager = IcebergTestHarness::builder()
            .with_column_stats_enabled()
            .with_eager_file_planning()
            .build()
            .await?;
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
        Ok(())
    }

    #[tokio::test]
    async fn eager_planning_reuses_files_on_distributed_workers() -> Result<()> {
        let lazy = IcebergTestHarness::builder()
            .with_workers(2)
            .with_column_stats_enabled()
            .build()
            .await?;
        let eager = IcebergTestHarness::builder()
            .with_workers(2)
            .with_column_stats_enabled()
            .with_eager_file_planning()
            .build()
            .await?;
        let query = "SELECT pickup_date, COUNT(*) AS trips FROM taxi \
                     WHERE pickup_date <= DATE '2024-01-11' \
                     GROUP BY pickup_date ORDER BY pickup_date";
        assert_eq!(lazy.query(query).await?.1, eager.query(query).await?.1);
        Ok(())
    }

    #[tokio::test]
    async fn eager_scan_can_be_executed_again() -> Result<()> {
        let harness = IcebergTestHarness::builder()
            .with_eager_file_planning()
            .build()
            .await?;
        let scan = harness.scan().await?;
        for _ in 0..2 {
            let batches = collect(Arc::clone(&scan), Arc::new(TaskContext::default())).await?;
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                175_000
            );
        }
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
            .configure_iceberg(|config| config.eager_file_planning = true)
            .configure_session(|state| Ok(state.with_runtime_env(runtime)))?
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
    async fn lazy_feeds_are_claimed_once_and_clones_get_fresh_execution() -> Result<()> {
        let harness = IcebergTestHarness::new().await?;
        let scan = harness.scan().await?;
        let partitions = scan.properties().partitioning.partition_count();
        let provider = scan
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .data_source()
            .downcast_ref::<LazyIcebergDataSource>()
            .unwrap()
            .feed()
            .inner()
            .unwrap();
        let first = read_feed(provider, partitions).await?;
        assert_eq!(first.len(), 7);
        assert!(provider.feed(0, Arc::new(TaskContext::default())).is_err());
        assert_eq!(read_feed(&provider.clone(), partitions).await?, first);
        Ok(())
    }

    async fn read_feed(provider: &IcebergWorkUnitFeed, partitions: usize) -> Result<Vec<Vec<u8>>> {
        let context = Arc::new(TaskContext::default());
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
}
