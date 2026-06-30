#[cfg(all(feature = "tpcds", test))]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::common::plan_err;
    use datafusion::error::Result;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::property_based::{
        compare_ordering, compare_result_set,
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, display_plan_ascii,
    };
    use datafusion_distributed_benchmarks::datasets::{register_tables, tpcds};
    use std::fs;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::OnceCell;

    const ADAPTIVE_ENV_VAR: &str = "ADAPTIVE";
    const NUM_WORKERS: usize = 4;
    const PARTITIONS: usize = 3;
    const FILE_SCAN_CONFIG_BYTES_PER_PARTITION: usize = 1;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 2.0;
    const SF: f64 = 1.0;
    const PARQUET_PARTITIONS: usize = 4;

    #[tokio::test]
    async fn test_tpcds_shard01_q1() -> Result<()> {
        test_tpcds_query("q1").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q2() -> Result<()> {
        test_tpcds_query("q2").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q3() -> Result<()> {
        test_tpcds_query("q3").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q4() -> Result<()> {
        test_tpcds_query("q4").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q5() -> Result<()> {
        test_tpcds_query("q5").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q6() -> Result<()> {
        test_tpcds_query("q6").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q7() -> Result<()> {
        test_tpcds_query("q7").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q8() -> Result<()> {
        test_tpcds_query("q8").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q9() -> Result<()> {
        test_tpcds_query("q9").await
    }

    #[tokio::test]
    async fn test_tpcds_shard01_q10() -> Result<()> {
        test_tpcds_query("q10").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q11() -> Result<()> {
        test_tpcds_query("q11").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q12() -> Result<()> {
        test_tpcds_query("q12").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q13() -> Result<()> {
        test_tpcds_query("q13").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q14() -> Result<()> {
        test_tpcds_query("q14").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q15() -> Result<()> {
        test_tpcds_query("q15").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q16() -> Result<()> {
        test_tpcds_query("q16").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q17() -> Result<()> {
        test_tpcds_query("q17").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q18() -> Result<()> {
        test_tpcds_query("q18").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q19() -> Result<()> {
        test_tpcds_query("q19").await
    }

    #[tokio::test]
    async fn test_tpcds_shard02_q20() -> Result<()> {
        test_tpcds_query("q20").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q21() -> Result<()> {
        test_tpcds_query("q21").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q22() -> Result<()> {
        test_tpcds_query("q22").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q23() -> Result<()> {
        test_tpcds_query("q23").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q24() -> Result<()> {
        test_tpcds_query("q24").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q25() -> Result<()> {
        test_tpcds_query("q25").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q26() -> Result<()> {
        test_tpcds_query("q26").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q27() -> Result<()> {
        test_tpcds_query("q27").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q28() -> Result<()> {
        test_tpcds_query("q28").await
    }

    #[tokio::test]
    async fn test_tpcds_shard03_q29() -> Result<()> {
        test_tpcds_query("q29").await
    }

    #[tokio::test]
    #[ignore = "fails on CI but works locally, see https://github.com/datafusion-contrib/datafusion-distributed/pull/452#issuecomment-4439115012"]
    async fn test_tpcds_shard03_q30() -> Result<()> {
        test_tpcds_query("q30").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q31() -> Result<()> {
        test_tpcds_query("q31").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q32() -> Result<()> {
        test_tpcds_query("q32").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q33() -> Result<()> {
        test_tpcds_query("q33").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q34() -> Result<()> {
        test_tpcds_query("q34").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q35() -> Result<()> {
        test_tpcds_query("q35").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q36() -> Result<()> {
        test_tpcds_query("q36").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q37() -> Result<()> {
        test_tpcds_query("q37").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q38() -> Result<()> {
        test_tpcds_query("q38").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q39() -> Result<()> {
        test_tpcds_query("q39").await
    }

    #[tokio::test]
    async fn test_tpcds_shard04_q40() -> Result<()> {
        test_tpcds_query("q40").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q41() -> Result<()> {
        test_tpcds_query("q41").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q42() -> Result<()> {
        test_tpcds_query("q42").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q43() -> Result<()> {
        test_tpcds_query("q43").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q44() -> Result<()> {
        test_tpcds_query("q44").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q45() -> Result<()> {
        test_tpcds_query("q45").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q46() -> Result<()> {
        test_tpcds_query("q46").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q47() -> Result<()> {
        test_tpcds_query("q47").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q48() -> Result<()> {
        test_tpcds_query("q48").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q49() -> Result<()> {
        test_tpcds_query("q49").await
    }

    #[tokio::test]
    async fn test_tpcds_shard05_q50() -> Result<()> {
        test_tpcds_query("q50").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q51() -> Result<()> {
        test_tpcds_query("q51").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q52() -> Result<()> {
        test_tpcds_query("q52").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q53() -> Result<()> {
        test_tpcds_query("q53").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q54() -> Result<()> {
        test_tpcds_query("q54").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q55() -> Result<()> {
        test_tpcds_query("q55").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q56() -> Result<()> {
        test_tpcds_query("q56").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q57() -> Result<()> {
        test_tpcds_query("q57").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q58() -> Result<()> {
        test_tpcds_query("q58").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q59() -> Result<()> {
        test_tpcds_query("q59").await
    }

    #[tokio::test]
    async fn test_tpcds_shard06_q60() -> Result<()> {
        test_tpcds_query("q60").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q61() -> Result<()> {
        test_tpcds_query("q61").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q62() -> Result<()> {
        test_tpcds_query("q62").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q63() -> Result<()> {
        test_tpcds_query("q63").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q64() -> Result<()> {
        test_tpcds_query("q64").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q65() -> Result<()> {
        test_tpcds_query("q65").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q66() -> Result<()> {
        test_tpcds_query("q66").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q67() -> Result<()> {
        test_tpcds_query("q67").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q68() -> Result<()> {
        test_tpcds_query("q68").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q69() -> Result<()> {
        test_tpcds_query("q69").await
    }

    #[tokio::test]
    async fn test_tpcds_shard07_q70() -> Result<()> {
        test_tpcds_query("q70").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q71() -> Result<()> {
        test_tpcds_query("q71").await
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
        test_tpcds_query("q72").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q73() -> Result<()> {
        test_tpcds_query("q73").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q74() -> Result<()> {
        test_tpcds_query("q74").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q75() -> Result<()> {
        test_tpcds_query("q75").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q76() -> Result<()> {
        test_tpcds_query("q76").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q77() -> Result<()> {
        test_tpcds_query("q77").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q78() -> Result<()> {
        test_tpcds_query("q78").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q79() -> Result<()> {
        test_tpcds_query("q79").await
    }

    #[tokio::test]
    async fn test_tpcds_shard08_q80() -> Result<()> {
        test_tpcds_query("q80").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q81() -> Result<()> {
        test_tpcds_query("q81").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q82() -> Result<()> {
        test_tpcds_query("q82").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q83() -> Result<()> {
        test_tpcds_query("q83").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q84() -> Result<()> {
        test_tpcds_query("q84").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q85() -> Result<()> {
        test_tpcds_query("q85").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q86() -> Result<()> {
        test_tpcds_query("q86").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q87() -> Result<()> {
        test_tpcds_query("q87").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q88() -> Result<()> {
        test_tpcds_query("q88").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q89() -> Result<()> {
        test_tpcds_query("q89").await
    }

    #[tokio::test]
    async fn test_tpcds_shard09_q90() -> Result<()> {
        test_tpcds_query("q90").await
    }

    #[tokio::test]
    #[ignore = "Query q91 did not get distributed"]
    async fn test_tpcds_shard10_q91() -> Result<()> {
        test_tpcds_query("q91").await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q92() -> Result<()> {
        test_tpcds_query("q92").await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q93() -> Result<()> {
        test_tpcds_query("q93").await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q94() -> Result<()> {
        test_tpcds_query("q94").await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q95() -> Result<()> {
        test_tpcds_query("q95").await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q96() -> Result<()> {
        test_tpcds_query("q96").await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q97() -> Result<()> {
        test_tpcds_query("q97").await
    }

    #[tokio::test]
    async fn test_tpcds_shard10_q98() -> Result<()> {
        test_tpcds_query("q98").await
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
        test_tpcds_query("q99").await
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

    async fn test_tpcds_query(query_id: &str) -> Result<()> {
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
        d_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;
        let mut d_ctx = d_ctx
            .with_distributed_file_scan_config_bytes_per_partition(
                FILE_SCAN_CONFIG_BYTES_PER_PARTITION,
            )?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?
            .with_distributed_broadcast_joins(true)?;
        if std::env::var(ADAPTIVE_ENV_VAR).unwrap_or_default() == "true" {
            d_ctx.set_distributed_dynamic_task_count(true)?;
        }

        register_tables(&s_ctx, &data_dir).await?;
        register_tables(&d_ctx, &data_dir).await?;

        let (s_plan, s_results) = run(&s_ctx, &query_sql).await;
        let (d_plan, d_results) = run(&d_ctx, &query_sql).await;

        if !d_plan.is::<DistributedExec>() {
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
                compare_result_set(&d_results, &s_results)
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
