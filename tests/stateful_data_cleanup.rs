#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tests {
    use datafusion::common::instant::Instant;
    use datafusion::error::Result;
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::{benchmarks_common, tpch};
    use datafusion_distributed::{DefaultSessionBuilder, DistributedExt};
    use futures::TryStreamExt;
    use std::fs;
    use std::path::Path;
    use std::time::Duration;
    use tokio::sync::OnceCell;
    use tokio::time::timeout;

    const NUM_WORKERS: usize = 4;
    const TPCH_SCALE_FACTOR: f64 = 1.0;
    const TPCH_DATA_PARTS: i32 = 16;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 1.0;

    #[tokio::test]
    async fn no_pending_tasks_if_query_completes() -> Result<()> {
        let (d_ctx, _guard, workers) =
            start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        run_tpch_query(d_ctx, "q1").await?;

        for (i, worker) in workers.iter().enumerate() {
            let tasks_running = worker.tasks_running().await;
            assert_eq!(
                tasks_running, 0,
                "Expected Worker {i} to have 0 tasks running, but got {tasks_running}"
            )
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn no_pending_tasks_if_query_aborts() -> Result<()> {
        let (d_ctx, _guard, workers) =
            start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;

        let _ = timeout(Duration::from_millis(100), run_tpch_query(d_ctx, "q1")).await;

        let start = Instant::now();
        let mut tasks_running = 0;
        while start.elapsed() < Duration::from_secs(5) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            tasks_running = 0;
            for worker in &workers {
                tasks_running += worker.tasks_running().await;
            }
            if tasks_running == 0 {
                return Ok(());
            }
        }

        assert_eq!(
            tasks_running, 0,
            "Expected to have 0 tasks running, but got {tasks_running}"
        );

        Ok(())
    }

    async fn run_tpch_query(d_ctx: SessionContext, query_id: &str) -> Result<()> {
        let data_dir = ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;

        let query_sql = tpch::get_query(query_id)?;

        let d_ctx = d_ctx
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?;

        benchmarks_common::register_tables(&d_ctx, &data_dir).await?;

        let df = d_ctx.sql(&query_sql).await?;
        let task_ctx = d_ctx.task_ctx();
        let plan = df.create_physical_plan().await?;

        let stream = execute_stream(plan.clone(), task_ctx)?;
        stream.try_collect::<Vec<_>>().await?;

        Ok(())
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    // ensure_tpch_data initializes the TPCH data on disk.
    pub async fn ensure_tpch_data(sf: f64, parts: i32) -> std::path::PathBuf {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("testdata/tpch/stateful_data_cleanup_sf{sf}"));
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    tpch::generate_tpch_data(&data_dir, sf, parts);
                }
            })
            .await;
        data_dir
    }
}
