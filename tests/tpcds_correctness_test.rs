#[cfg(all(feature = "integration", feature = "tpcds", test))]
mod tests {
    use datafusion::arrow::array::RecordBatch;
    use datafusion::common::plan_err;
    use datafusion::error::Result;
    use datafusion::physical_plan::{ExecutionPlan, collect};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::property_based::{
        compare_ordering, compare_result_set,
    };
    use datafusion_distributed::test_utils::{benchmarks_common, tpcds};
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, display_plan_ascii,
    };
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
    async fn test_tpcds_1() -> Result<()> {
        test_tpcds_query("q1").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_2() -> Result<()> {
        test_tpcds_query("q2").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_3() -> Result<()> {
        test_tpcds_query("q3").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_4() -> Result<()> {
        test_tpcds_query("q4").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_5() -> Result<()> {
        test_tpcds_query("q5").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_6() -> Result<()> {
        test_tpcds_query("q6").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_7() -> Result<()> {
        test_tpcds_query("q7").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_8() -> Result<()> {
        test_tpcds_query("q8").await
    }

    #[tokio::test]
    #[ignore = "expected no error but got: Arrow error: Invalid argument error: must either specify a row count or at least one column"]
    async fn test_tpcds_9() -> Result<()> {
        test_tpcds_query("q9").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_10() -> Result<()> {
        test_tpcds_query("q10").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_11() -> Result<()> {
        test_tpcds_query("q11").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_12() -> Result<()> {
        test_tpcds_query("q12").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_13() -> Result<()> {
        test_tpcds_query("q13").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_14() -> Result<()> {
        test_tpcds_query("q14").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_15() -> Result<()> {
        test_tpcds_query("q15").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_16() -> Result<()> {
        test_tpcds_query("q16").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_17() -> Result<()> {
        test_tpcds_query("q17").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_18() -> Result<()> {
        test_tpcds_query("q18").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_19() -> Result<()> {
        test_tpcds_query("q19").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_20() -> Result<()> {
        test_tpcds_query("q20").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_21() -> Result<()> {
        test_tpcds_query("q21").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_22() -> Result<()> {
        test_tpcds_query("q22").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_23() -> Result<()> {
        test_tpcds_query("q23").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_24() -> Result<()> {
        test_tpcds_query("q24").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_25() -> Result<()> {
        test_tpcds_query("q25").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_26() -> Result<()> {
        test_tpcds_query("q26").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_27() -> Result<()> {
        test_tpcds_query("q27").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_28() -> Result<()> {
        test_tpcds_query("q28").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_29() -> Result<()> {
        test_tpcds_query("q29").await
    }

    #[tokio::test]
    #[ignore = "Fails with column 'c_last_review_date_sk' not found"]
    async fn test_tpcds_30() -> Result<()> {
        test_tpcds_query("q30").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_31() -> Result<()> {
        test_tpcds_query("q31").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_32() -> Result<()> {
        test_tpcds_query("q32").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_33() -> Result<()> {
        test_tpcds_query("q33").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_34() -> Result<()> {
        test_tpcds_query("q34").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_35() -> Result<()> {
        test_tpcds_query("q35").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_36() -> Result<()> {
        test_tpcds_query("q36").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_37() -> Result<()> {
        test_tpcds_query("q37").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_38() -> Result<()> {
        test_tpcds_query("q38").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_39() -> Result<()> {
        test_tpcds_query("q39").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_40() -> Result<()> {
        test_tpcds_query("q40").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_41() -> Result<()> {
        test_tpcds_query("q41").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_42() -> Result<()> {
        test_tpcds_query("q42").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_43() -> Result<()> {
        test_tpcds_query("q43").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_44() -> Result<()> {
        test_tpcds_query("q44").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_45() -> Result<()> {
        test_tpcds_query("q45").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_46() -> Result<()> {
        test_tpcds_query("q46").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_47() -> Result<()> {
        test_tpcds_query("q47").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_48() -> Result<()> {
        test_tpcds_query("q48").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_49() -> Result<()> {
        test_tpcds_query("q49").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_50() -> Result<()> {
        test_tpcds_query("q50").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_51() -> Result<()> {
        test_tpcds_query("q51").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_52() -> Result<()> {
        test_tpcds_query("q52").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_53() -> Result<()> {
        test_tpcds_query("q53").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_54() -> Result<()> {
        test_tpcds_query("q54").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_55() -> Result<()> {
        test_tpcds_query("q55").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_56() -> Result<()> {
        test_tpcds_query("q56").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_57() -> Result<()> {
        test_tpcds_query("q57").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_58() -> Result<()> {
        test_tpcds_query("q58").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_59() -> Result<()> {
        test_tpcds_query("q59").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_60() -> Result<()> {
        test_tpcds_query("q60").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_61() -> Result<()> {
        test_tpcds_query("q61").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_62() -> Result<()> {
        test_tpcds_query("q62").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_63() -> Result<()> {
        test_tpcds_query("q63").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_64() -> Result<()> {
        test_tpcds_query("q64").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_65() -> Result<()> {
        test_tpcds_query("q65").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_66() -> Result<()> {
        test_tpcds_query("q66").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_67() -> Result<()> {
        test_tpcds_query("q67").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_68() -> Result<()> {
        test_tpcds_query("q68").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_69() -> Result<()> {
        test_tpcds_query("q69").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_70() -> Result<()> {
        test_tpcds_query("q70").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_71() -> Result<()> {
        test_tpcds_query("q71").await
    }

    #[tokio::test]
    // For some reason this test takes a ridiculous amount of time to execute. There might be
    // nothing wrong with it, and it just might be too heavy. The test passes, but it takes so
    // long to execute that it's not worth the time.
    #[ignore = "Query takes too long to execute"]
    async fn test_tpcds_72() -> Result<()> {
        test_tpcds_query("q72").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_73() -> Result<()> {
        test_tpcds_query("q73").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_74() -> Result<()> {
        test_tpcds_query("q74").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_75() -> Result<()> {
        test_tpcds_query("q75").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_76() -> Result<()> {
        test_tpcds_query("q76").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_77() -> Result<()> {
        test_tpcds_query("q77").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_78() -> Result<()> {
        test_tpcds_query("q78").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_79() -> Result<()> {
        test_tpcds_query("q79").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_80() -> Result<()> {
        test_tpcds_query("q80").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_81() -> Result<()> {
        test_tpcds_query("q81").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_82() -> Result<()> {
        test_tpcds_query("q82").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_83() -> Result<()> {
        test_tpcds_query("q83").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_84() -> Result<()> {
        test_tpcds_query("q84").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_85() -> Result<()> {
        test_tpcds_query("q85").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_86() -> Result<()> {
        test_tpcds_query("q86").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_87() -> Result<()> {
        test_tpcds_query("q87").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_88() -> Result<()> {
        test_tpcds_query("q88").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_89() -> Result<()> {
        test_tpcds_query("q89").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_90() -> Result<()> {
        test_tpcds_query("q90").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_91() -> Result<()> {
        test_tpcds_query("q91").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_92() -> Result<()> {
        test_tpcds_query("q92").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_93() -> Result<()> {
        test_tpcds_query("q93").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_94() -> Result<()> {
        test_tpcds_query("q94").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_95() -> Result<()> {
        test_tpcds_query("q95").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_96() -> Result<()> {
        test_tpcds_query("q96").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_97() -> Result<()> {
        test_tpcds_query("q97").await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_tpcds_98() -> Result<()> {
        test_tpcds_query("q98").await
    }

    #[tokio::test]
    // For some reason this test takes a ridiculous amount of time to execute. There might be
    // nothing wrong with it, and it just might be too heavy. The test passes, but it takes so
    // long to execute that it's not worth the time.
    #[ignore = "Query takes too long to execute"]
    async fn test_tpcds_99() -> Result<()> {
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
        let (d_ctx, _guard) = start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        let d_ctx = d_ctx
            .with_distributed_files_per_task(FILES_PER_TASK)?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?;

        benchmarks_common::register_tables(&s_ctx, &data_dir).await?;
        benchmarks_common::register_tables(&d_ctx, &data_dir).await?;

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
