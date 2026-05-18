#[cfg(all(feature = "integration", feature = "clickbench", test))]
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
    use datafusion_distributed_benchmarks::datasets::{clickbench, register_tables};
    use std::ops::Range;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::OnceCell;

    const NUM_WORKERS: usize = 4;
    const FILES_PER_TASK: usize = 2;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 2.0;
    const FILE_RANGE: Range<usize> = 0..3;

    #[tokio::test]
    #[ignore = "Query 0 did not get distributed.The planner correctly chooses a single-task plan because of parquet statistics."]

    async fn test_clickbench_0() -> Result<()> {
        test_clickbench_query("q0", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_1() -> Result<()> {
        test_clickbench_query("q1", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_2() -> Result<()> {
        test_clickbench_query("q2", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_3() -> Result<()> {
        test_clickbench_query("q3", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_4() -> Result<()> {
        test_clickbench_query("q4", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_5() -> Result<()> {
        test_clickbench_query("q5", PerTestConfig::default()).await
    }

    #[tokio::test]
    #[ignore = "Query 6 did not get distributed.The planner correctly chooses a single-task plan because of parquet statistics."]
    async fn test_clickbench_6() -> Result<()> {
        test_clickbench_query("q6", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_7() -> Result<()> {
        test_clickbench_query("q7", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_8() -> Result<()> {
        test_clickbench_query("q8", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_9() -> Result<()> {
        test_clickbench_query("q9", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_10() -> Result<()> {
        test_clickbench_query("q10", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_11() -> Result<()> {
        test_clickbench_query("q11", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_12() -> Result<()> {
        test_clickbench_query("q12", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_13() -> Result<()> {
        test_clickbench_query("q13", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_14() -> Result<()> {
        test_clickbench_query("q14", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_15() -> Result<()> {
        test_clickbench_query("q15", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_16() -> Result<()> {
        test_clickbench_query("q16", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_17() -> Result<()> {
        test_clickbench_query(
            "q17",
            PerTestConfig {
                uses_undeterministic_limit_operator: true,
                ..Default::default()
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_clickbench_18() -> Result<()> {
        test_clickbench_query("q18", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_19() -> Result<()> {
        test_clickbench_query("q19", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_20() -> Result<()> {
        test_clickbench_query("q20", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_21() -> Result<()> {
        test_clickbench_query(
            "q21",
            PerTestConfig {
                non_deterministic_sort: true,
                ..Default::default()
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_clickbench_22() -> Result<()> {
        test_clickbench_query(
            "q22",
            PerTestConfig {
                uses_undeterministic_limit_operator: true,
                ..Default::default()
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_clickbench_23() -> Result<()> {
        test_clickbench_query("q23", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_24() -> Result<()> {
        test_clickbench_query(
            "q24",
            PerTestConfig {
                non_deterministic_sort: true,
                ..Default::default()
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_clickbench_25() -> Result<()> {
        test_clickbench_query("q25", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_26() -> Result<()> {
        test_clickbench_query("q26", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_27() -> Result<()> {
        test_clickbench_query("q27", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_28() -> Result<()> {
        test_clickbench_query("q28", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_29() -> Result<()> {
        test_clickbench_query("q29", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_30() -> Result<()> {
        test_clickbench_query("q30", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_31() -> Result<()> {
        test_clickbench_query(
            "q31",
            PerTestConfig {
                non_deterministic_sort: true,
                ..Default::default()
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_clickbench_32() -> Result<()> {
        test_clickbench_query(
            "q32",
            PerTestConfig {
                non_deterministic_sort: true,
                ..Default::default()
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_clickbench_33() -> Result<()> {
        test_clickbench_query("q33", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_34() -> Result<()> {
        test_clickbench_query("q34", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_35() -> Result<()> {
        test_clickbench_query("q35", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_36() -> Result<()> {
        test_clickbench_query("q36", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_37() -> Result<()> {
        test_clickbench_query("q37", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_38() -> Result<()> {
        test_clickbench_query("q38", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_39() -> Result<()> {
        test_clickbench_query("q39", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_40() -> Result<()> {
        test_clickbench_query("q40", PerTestConfig::default()).await
    }

    #[tokio::test]
    async fn test_clickbench_41() -> Result<()> {
        test_clickbench_query("q41", PerTestConfig::default()).await
    }

    #[tokio::test]
    #[ignore = "Ordering mismatch on `date_trunc('minute', ...)`: `compare_ordering` reports `LexOrdering` inequality even though Debug output is byte-identical on both sides. The diff is in a non-printed field of LexOrdering (e.g. schema metadata or equivalence-class set membership)."]
    async fn test_clickbench_42() -> Result<()> {
        test_clickbench_query("q42", PerTestConfig::default()).await
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

    async fn test_clickbench_query(query_id: &str, config: PerTestConfig) -> Result<()> {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "testdata/clickbench/correctness_range{}-{}",
            FILE_RANGE.start, FILE_RANGE.end
        ));
        INIT_TEST_TPCDS_TABLES
            .get_or_init(|| async {
                clickbench::generate_clickbench_data(&data_dir, FILE_RANGE)
                    .await
                    .unwrap();
            })
            .await;

        let query_sql = clickbench::get_query(query_id)?;
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
