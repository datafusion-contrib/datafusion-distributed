#[cfg(all(feature = "integration", feature = "tpcds", test))]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::common::plan_err;
    use datafusion::error::Result;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::property_based::{
        PerTestConfig, compare_ordering, compare_result_set,
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, display_plan_ascii,
    };
    use datafusion_distributed_benchmarks::datasets::{register_tables, tpcds};
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::OnceCell;

    const NUM_WORKERS: usize = 4;
    const FILES_PER_TASK: usize = 2;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 2.0;
    const SF: f64 = 1.0;
    const PARQUET_PARTITIONS: usize = 4;

    #[tokio::test]
    async fn test_tpcds_shard01_q1() -> Result<()> {
        test_tpcds_query("q1", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q2() -> Result<()> {
        test_tpcds_query("q2", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q3() -> Result<()> {
        test_tpcds_query("q3", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q4() -> Result<()> {
        test_tpcds_query("q4", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q5() -> Result<()> {
        test_tpcds_query("q5", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q6() -> Result<()> {
        test_tpcds_query("q6", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q7() -> Result<()> {
        test_tpcds_query("q7", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q8() -> Result<()> {
        test_tpcds_query("q8", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q9() -> Result<()> {
        test_tpcds_query("q9", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q10() -> Result<()> {
        test_tpcds_query("q10", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q11() -> Result<()> {
        test_tpcds_query("q11", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q12() -> Result<()> {
        test_tpcds_query("q12", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q13() -> Result<()> {
        test_tpcds_query("q13", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q14() -> Result<()> {
        test_tpcds_query("q14", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q15() -> Result<()> {
        test_tpcds_query("q15", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q16() -> Result<()> {
        test_tpcds_query("q16", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q17() -> Result<()> {
        test_tpcds_query("q17", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q18() -> Result<()> {
        test_tpcds_query("q18", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q19() -> Result<()> {
        test_tpcds_query("q19", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q20() -> Result<()> {
        test_tpcds_query("q20", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q21() -> Result<()> {
        test_tpcds_query("q21", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q22() -> Result<()> {
        test_tpcds_query("q22", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q23() -> Result<()> {
        test_tpcds_query("q23", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q24() -> Result<()> {
        test_tpcds_query("q24", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q25() -> Result<()> {
        test_tpcds_query("q25", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q26() -> Result<()> {
        test_tpcds_query("q26", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q27() -> Result<()> {
        test_tpcds_query("q27", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q28() -> Result<()> {
        test_tpcds_query("q28", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q29() -> Result<()> {
        test_tpcds_query("q29", PerTestConfig::default()).await
    }

    #[tokio::test]
    #[ignore = "fails on CI but works locally, see https://github.com/datafusion-contrib/datafusion-distributed/pull/452#issuecomment-4439115012"]
    async fn test_tpcds_shard03_q30() -> Result<()> {
        test_tpcds_query("q30", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q31() -> Result<()> {
        test_tpcds_query("q31", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q32() -> Result<()> {
        test_tpcds_query("q32", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q33() -> Result<()> {
        test_tpcds_query("q33", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q34() -> Result<()> {
        test_tpcds_query("q34", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q35() -> Result<()> {
        test_tpcds_query("q35", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q36() -> Result<()> {
        test_tpcds_query("q36", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q37() -> Result<()> {
        test_tpcds_query("q37", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q38() -> Result<()> {
        test_tpcds_query("q38", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q39() -> Result<()> {
        test_tpcds_query("q39", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q40() -> Result<()> {
        test_tpcds_query("q40", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q41() -> Result<()> {
        test_tpcds_query("q41", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q42() -> Result<()> {
        test_tpcds_query("q42", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q43() -> Result<()> {
        test_tpcds_query("q43", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q44() -> Result<()> {
        test_tpcds_query("q44", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q45() -> Result<()> {
        test_tpcds_query("q45", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q46() -> Result<()> {
        test_tpcds_query("q46", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q47() -> Result<()> {
        test_tpcds_query("q47", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q48() -> Result<()> {
        test_tpcds_query("q48", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q49() -> Result<()> {
        test_tpcds_query("q49", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q50() -> Result<()> {
        test_tpcds_query("q50", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q51() -> Result<()> {
        test_tpcds_query("q51", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q52() -> Result<()> {
        test_tpcds_query("q52", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q53() -> Result<()> {
        test_tpcds_query("q53", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q54() -> Result<()> {
        test_tpcds_query("q54", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q55() -> Result<()> {
        test_tpcds_query("q55", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q56() -> Result<()> {
        test_tpcds_query("q56", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q57() -> Result<()> {
        test_tpcds_query("q57", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q58() -> Result<()> {
        test_tpcds_query("q58", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q59() -> Result<()> {
        test_tpcds_query("q59", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q60() -> Result<()> {
        test_tpcds_query("q60", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q61() -> Result<()> {
        test_tpcds_query("q61", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q62() -> Result<()> {
        test_tpcds_query("q62", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q63() -> Result<()> {
        test_tpcds_query("q63", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q64() -> Result<()> {
        test_tpcds_query("q64", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q65() -> Result<()> {
        test_tpcds_query("q65", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q66() -> Result<()> {
        test_tpcds_query("q66", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q67() -> Result<()> {
        test_tpcds_query("q67", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q68() -> Result<()> {
        test_tpcds_query("q68", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q69() -> Result<()> {
        test_tpcds_query("q69", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q70() -> Result<()> {
        test_tpcds_query("q70", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q71() -> Result<()> {
        test_tpcds_query("q71", PerTestConfig::default()).await
    }

    #[tokio::test]
    // For some reason this test takes a ridiculous amount of time to execute. There might be
    // nothing wrong with it, and it just might be too heavy. The test passes, but it takes so
    // long to execute that it's not worth the time. Gated behind the `slow-tests` feature.
    #[cfg_attr(
        not(feature = "slow-tests"),
        ignore = "Query takes too long to execute"
    )]
    async fn test_tpcds_shard08_q72() -> Result<()> {
        test_tpcds_query("q72", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q73() -> Result<()> {
        test_tpcds_query("q73", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q74() -> Result<()> {
        test_tpcds_query("q74", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q75() -> Result<()> {
        test_tpcds_query("q75", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q76() -> Result<()> {
        test_tpcds_query("q76", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q77() -> Result<()> {
        test_tpcds_query("q77", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q78() -> Result<()> {
        test_tpcds_query("q78", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q79() -> Result<()> {
        test_tpcds_query("q79", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q80() -> Result<()> {
        test_tpcds_query("q80", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q81() -> Result<()> {
        test_tpcds_query("q81", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q82() -> Result<()> {
        test_tpcds_query("q82", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q83() -> Result<()> {
        test_tpcds_query("q83", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q84() -> Result<()> {
        test_tpcds_query("q84", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q85() -> Result<()> {
        test_tpcds_query("q85", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q86() -> Result<()> {
        test_tpcds_query("q86", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q87() -> Result<()> {
        test_tpcds_query("q87", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q88() -> Result<()> {
        test_tpcds_query("q88", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q89() -> Result<()> {
        test_tpcds_query("q89", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q90() -> Result<()> {
        test_tpcds_query("q90", PerTestConfig::default()).await
    }

    #[tokio::test]
    #[ignore = "Query q91 did not get distributed"]
    async fn test_tpcds_shard10_q91() -> Result<()> {
        test_tpcds_query("q91", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q92() -> Result<()> {
        test_tpcds_query("q92", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q93() -> Result<()> {
        test_tpcds_query("q93", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q94() -> Result<()> {
        test_tpcds_query("q94", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q95() -> Result<()> {
        test_tpcds_query("q95", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q96() -> Result<()> {
        test_tpcds_query("q96", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q97() -> Result<()> {
        test_tpcds_query("q97", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q98() -> Result<()> {
        test_tpcds_query("q98", PerTestConfig::default()).await
    }

    #[tokio::test]
    // For some reason this test takes a ridiculous amount of time to execute. There might be
    // nothing wrong with it, and it just might be too heavy. The test passes, but it takes so
    // long to execute that it's not worth the time. Gated behind the `slow-tests` feature.
    #[cfg_attr(
        not(feature = "slow-tests"),
        ignore = "Query takes too long to execute"
    )]
    async fn test_tpcds_shard10_q99() -> Result<()> {
        test_tpcds_query("q99", PerTestConfig::default()).await
    }

    static INIT_TEST_TPCDS_TABLES: OnceCell<()> = OnceCell::const_new();

    async fn run(
        ctx: &SessionContext,
        query_sql: &str,
    ) -> (Arc<dyn ExecutionPlan>, Arc<Result<Vec<RecordBatch>>>) {
        let df = ctx.sql(query_sql).await.unwrap();
        let task_ctx = ctx.task_ctx();
        let plan = df.create_physical_plan().await.unwrap();
        (plan.clone(), Arc::new(collect(plan, task_ctx).await)) // Collect execution errors, do not unwrap.
    }

    async fn test_tpcds_query(query_id: &str, config: PerTestConfig) -> Result<()> {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "testdata/tpcds/correctness_sf{SF}_partitions{PARQUET_PARTITIONS}"
        ));
        INIT_TEST_TPCDS_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap_or(false) {
                    tpcds::generate_data(&data_dir, SF, PARQUET_PARTITIONS)
                        .await
                        .unwrap();
                }
            })
            .await;

        let query_sql = tpcds::get_query(query_id)?;
        // Create a single node context to compare results to.
        let s_ctx = SessionContext::new();

        // Make distributed localhost context to run queries
        let d_ctx = start_in_memory_context(NUM_WORKERS, DefaultSessionBuilder).await;
        let d_ctx = d_ctx
            .with_distributed_files_per_task(FILES_PER_TASK)?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?
            .with_distributed_broadcast_joins(true)?;

        register_tables(&s_ctx, &data_dir).await?;
        register_tables(&d_ctx, &data_dir).await?;

        let (s_plan, s_results) = run(&s_ctx, &query_sql).await;
        let (d_plan, d_results) = run(&d_ctx, &query_sql).await;

        if !d_plan.as_any().is::<DistributedExec>() {
            return plan_err!("Query {query_id} did not get distributed");
        }
        let display = display_plan_ascii(d_plan.as_ref(), false);
        println!("Query {query_id}:\n{display}");

        // The comparison functions can be computationally expensive, so we spawn them in tokio
        // blocking tasks so that they do not block the tokio runtime.
        let compare_result_set = {
            let d_results = d_results.clone();
            let s_results = s_results.clone();
            tokio::task::spawn_blocking(move || async move {
                compare_result_set(&d_results, &s_results, &config)
            })
        };
        let compare_ordering = {
            let d_results = d_results.clone();
            tokio::task::spawn_blocking(move || async move {
                compare_ordering(d_plan, s_plan, &d_results)
            })
        };
        compare_result_set.await.unwrap().await?;
        compare_ordering.await.unwrap().await?;

        Ok(())
    }
}
