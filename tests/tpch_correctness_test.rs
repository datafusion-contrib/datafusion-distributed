#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tests {
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::DefaultSessionBuilder;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::{benchmarks_common, tpch};
    use futures::TryStreamExt;
    use std::error::Error;
    use std::fmt::Display;
    use std::fs;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const PARTITIONS: usize = 6;
    const TPCH_SCALE_FACTOR: f64 = 1.0;
    const TPCH_DATA_PARTS: i32 = 16;

    #[tokio::test]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q1")?).await
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q2")?).await
    }

    #[tokio::test]
    async fn test_tpch_3() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q3")?).await
    }

    #[tokio::test]
    async fn test_tpch_4() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q4")?).await
    }

    #[tokio::test]
    async fn test_tpch_5() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q5")?).await
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q6")?).await
    }

    #[tokio::test]
    async fn test_tpch_7() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q7")?).await
    }

    #[tokio::test]
    async fn test_tpch_8() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q8")?).await
    }

    #[tokio::test]
    async fn test_tpch_9() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q9")?).await
    }

    #[tokio::test]
    async fn test_tpch_10() -> Result<(), Box<dyn Error>> {
        let sql = tpch::get_query("q10")?;
        // There is a chance that this query returns non-deterministic results if two entries
        // happen to have the exact same revenue. With small scales, this never happens, but with
        // bigger scales, this is very likely to happen.
        // This extra ordering accounts for it.
        let sql = sql.replace("revenue desc", "revenue, c_acctbal desc");
        test_tpch_query(sql).await
    }

    #[tokio::test]
    async fn test_tpch_11() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q11")?).await
    }

    #[tokio::test]
    async fn test_tpch_12() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q12")?).await
    }

    #[tokio::test]
    async fn test_tpch_13() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q13")?).await
    }

    #[tokio::test]
    async fn test_tpch_14() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q14")?).await
    }

    #[tokio::test]
    async fn test_tpch_15() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q15")?).await
    }

    #[tokio::test]
    async fn test_tpch_16() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q16")?).await
    }

    #[tokio::test]
    async fn test_tpch_17() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q17")?).await
    }

    #[tokio::test]
    async fn test_tpch_18() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q18")?).await
    }

    #[tokio::test]
    async fn test_tpch_19() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q19")?).await
    }

    #[tokio::test]
    async fn test_tpch_20() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q20")?).await
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q21")?).await
    }

    #[tokio::test]
    async fn test_tpch_22() -> Result<(), Box<dyn Error>> {
        test_tpch_query(tpch::get_query("q22")?).await
    }

    async fn test_tpch_query(sql: String) -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context(4, DefaultSessionBuilder).await;

        let results_d = run_tpch_query(ctx, sql.clone()).await?;
        let results_s = run_tpch_query(SessionContext::new(), sql).await?;

        pretty_assertions::assert_eq!(results_d.to_string(), results_s.to_string(),);
        Ok(())
    }

    // test_non_distributed_consistency runs each TPC-H query twice - once in a distributed manner
    // and once in a non-distributed manner. For each query, it asserts that the results are identical.
    async fn run_tpch_query(
        ctx: SessionContext,
        sql: String,
    ) -> Result<impl Display, Box<dyn Error>> {
        let data_dir = ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;

        benchmarks_common::register_tables(&ctx, &data_dir).await?;

        // Query 15 has three queries in it, one creating the view, the second
        // executing, which we want to capture the output of, and the third
        // tearing down the view
        let stream = if sql.starts_with("create view") {
            let queries: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            ctx.sql(queries[0]).await?.collect().await?;
            let df = ctx.sql(queries[1]).await?;
            let plan = df.create_physical_plan().await?;
            let stream = execute_stream(plan.clone(), ctx.task_ctx())?;
            ctx.sql(queries[2]).await?.collect().await?;

            stream
        } else {
            let df = ctx.sql(&sql).await?;
            let plan = df.create_physical_plan().await?;
            execute_stream(plan.clone(), ctx.task_ctx())?
        };

        let batches = stream.try_collect::<Vec<_>>().await?;

        Ok(arrow::util::pretty::pretty_format_batches(&batches)?)
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    // ensure_tpch_data initializes the TPCH data on disk.
    pub async fn ensure_tpch_data(sf: f64, parts: i32) -> std::path::PathBuf {
        let data_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("testdata/tpch/correctness_sf{sf}"));
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
