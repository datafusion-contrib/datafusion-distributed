#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tests {
    use datafusion::common::{Result, instant::Instant};
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{DefaultSessionBuilder, DistributedExt, Worker};
    use datafusion_distributed_benchmarks::datasets::{
        output::DatasetOutput, register_tables, tpch,
    };
    use futures::TryStreamExt;
    use std::fs;
    use std::path::Path;
    use std::time::Duration;
    use test_case::test_case;
    use tokio::{
        spawn,
        sync::OnceCell,
        time::{sleep, timeout},
    };

    const NUM_WORKERS: usize = 4;
    const TPCH_SCALE_FACTOR: f64 = 1.0;
    const TPCH_DATA_PARTS: usize = 16;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 1.0;

    #[test_case((false, false); "metrics_disabled_static_planner")]
    #[test_case((true, false); "metrics_enabled_static_planner")]
    #[test_case((false, true); "metrics_disabled_dynamic_planner")]
    #[test_case((true, true); "metrics_enabled_dynamic_planner")]
    #[tokio::test(flavor = "multi_thread")]
    async fn no_pending_tasks_if_dynamic_query_completes(
        (collect_metrics, adaptive): (bool, bool),
    ) -> Result<()> {
        let (mut d_ctx, _guard, workers) =
            start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        d_ctx.set_distributed_metrics_collection(collect_metrics)?;
        d_ctx.set_distributed_dynamic_task_count(adaptive)?;

        run_tpch_query(d_ctx, "q1").await?;

        assert_no_tasks_running_eventually(&workers).await;

        Ok(())
    }

    #[test_case((false, false); "metrics_disabled_static_planner")]
    #[test_case((true, false); "metrics_enabled_static_planner")]
    #[test_case((false, true); "metrics_disabled_dynamic_planner")]
    #[test_case((true, true); "metrics_enabled_dynamic_planner")]
    #[tokio::test(flavor = "multi_thread")]
    async fn no_pending_tasks_if_query_aborts(
        (collect_metrics, adaptive): (bool, bool),
    ) -> Result<()> {
        let (mut d_ctx, _guard, workers) =
            start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        d_ctx.set_distributed_metrics_collection(collect_metrics)?;
        d_ctx.set_distributed_dynamic_task_count(adaptive)?;

        let _ = timeout(Duration::from_millis(100), run_tpch_query(d_ctx, "q1")).await;

        assert_no_tasks_running_eventually(&workers).await;

        Ok(())
    }

    #[test_case((false, false); "metrics_disabled_static_planner")]
    #[test_case((true, false); "metrics_enabled_static_planner")]
    #[test_case((false, true); "metrics_disabled_dynamic_planner")]
    #[test_case((true, true); "metrics_enabled_dynamic_planner")]
    #[tokio::test(flavor = "multi_thread")]
    async fn cancellation_closes_coordinator_channels(
        (collect_metrics, adaptive): (bool, bool),
    ) -> Result<()> {
        let (mut d_ctx, _guard, workers) =
            start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        d_ctx.set_distributed_metrics_collection(collect_metrics)?;
        d_ctx.set_distributed_dynamic_task_count(adaptive)?;

        #[allow(clippy::disallowed_methods)]
        let execution = spawn(run_tpch_query(d_ctx, "q2"));

        timeout(Duration::from_secs(10), async {
            while coordinator_channels_running(&workers) == 0 {
                assert!(
                    !execution.is_finished(),
                    "query completed before opening a coordinator channel"
                );
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("query did not open a coordinator channel within 10 seconds");
        assert!(coordinator_channels_running(&workers) > 0);
        execution.abort();
        let error = timeout(Duration::from_secs(1), execution)
            .await
            .expect("cancelled query did not stop within one second")
            .expect_err("query completed before it was cancelled");
        assert!(error.is_cancelled());
        assert_no_tasks_running_eventually(&workers).await;

        Ok(())
    }

    /// Polls until every worker reports 0 running tasks and worker-to-coordinator streams, or fails
    /// after 5s. Cleanup is asynchronous after the query output is dropped, so it is not observable
    /// synchronously when the query future resolves.
    async fn assert_no_tasks_running_eventually(workers: &[Worker]) {
        let start = Instant::now();
        loop {
            let mut tasks_running = 0;
            for worker in workers {
                tasks_running += worker.tasks_running().await;
            }
            let channels_running = coordinator_channels_running(workers);
            if tasks_running == 0 && channels_running == 0 {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(5),
                "Expected no running tasks or coordinator channels, but still had \
                 {tasks_running} tasks and {channels_running} channels after 5s"
            );
            sleep(Duration::from_millis(50)).await;
        }
    }

    fn coordinator_channels_running(workers: &[Worker]) -> usize {
        workers
            .iter()
            .map(Worker::coordinator_channels_running)
            .sum()
    }

    async fn run_tpch_query(d_ctx: SessionContext, query_id: &str) -> Result<()> {
        let data_dir = ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;

        let query_sql = tpch::get_query(query_id)?;

        let d_ctx = d_ctx
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?;

        register_tables(&d_ctx, &data_dir).await?;

        let df = d_ctx.sql(&query_sql).await?;
        let task_ctx = d_ctx.task_ctx();
        let plan = df.create_physical_plan().await?;

        let stream = execute_stream(plan.clone(), task_ctx)?;
        stream.try_collect::<Vec<_>>().await?;

        Ok(())
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    pub async fn ensure_tpch_data(sf: f64, parts: usize) -> std::path::PathBuf {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("testdata/tpch/stateful_data_cleanup_sf{sf}"));
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    let output = DatasetOutput::new(data_dir.to_str().unwrap())
                        .await
                        .unwrap();
                    tpch::generate_data(&output, sf, parts, false)
                        .await
                        .expect("Failed to generate TPC-H data");
                }
            })
            .await;
        data_dir
    }
}
