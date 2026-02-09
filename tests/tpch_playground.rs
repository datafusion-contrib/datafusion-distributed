#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tests {
    use datafusion::physical_plan::collect;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::{benchmarks_common, tpch};
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, DistributedMetricsFormat, display_plan_ascii,
        rewrite_distributed_plan_with_metrics,
    };
    use std::error::Error;
    use std::fs;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const PARTITIONS: usize = 6;
    const BYTES_PROCESSED_PER_PARTITION: usize = 128 * 1024 * 1024;
    const TPCH_SCALE_FACTOR: f64 = 1.0;
    const TPCH_DATA_PARTS: i32 = 16;

    #[tokio::test]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q1").await
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q2").await
    }

    #[tokio::test]
    async fn test_tpch_3() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q3").await
    }

    #[tokio::test]
    async fn test_tpch_4() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q4").await
    }

    #[tokio::test]
    async fn test_tpch_5() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q5").await
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q6").await
    }

    #[tokio::test]
    async fn test_tpch_7() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q7").await
    }

    #[tokio::test]
    async fn test_tpch_8() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q8").await
    }

    #[tokio::test]
    async fn test_tpch_9() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q9").await
    }

    #[tokio::test]
    async fn test_tpch_10() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q10").await
    }

    #[tokio::test]
    async fn test_tpch_11() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q11").await
    }

    #[tokio::test]
    async fn test_tpch_12() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q12").await
    }

    #[tokio::test]
    async fn test_tpch_13() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q13").await
    }

    #[tokio::test]
    async fn test_tpch_14() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q14").await
    }

    #[tokio::test]
    async fn test_tpch_15() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q15").await
    }

    #[tokio::test]
    async fn test_tpch_16() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q16").await
    }

    #[tokio::test]
    async fn test_tpch_17() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q17").await
    }

    #[tokio::test]
    async fn test_tpch_18() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q18").await
    }

    #[tokio::test]
    async fn test_tpch_19() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q19").await
    }

    #[tokio::test]
    async fn test_tpch_20() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q20").await
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q21").await
    }

    #[tokio::test]
    async fn test_tpch_22() -> Result<(), Box<dyn Error>> {
        test_tpch_query("q22").await
    }

    // test_tpch_query generates and displays a distributed plan for each TPC-H query.
    async fn test_tpch_query(query_id: &str) -> Result<(), Box<dyn Error>> {
        let (d_ctx, _guard, _) = start_localhost_context(4, DefaultSessionBuilder).await;
        let d_ctx = d_ctx
            .with_distributed_broadcast_joins(true)?
            .with_distributed_bytes_processed_per_partition(BYTES_PROCESSED_PER_PARTITION)?;
        let data_dir = ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;
        let sql = tpch::get_query(query_id)?;
        d_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = PARTITIONS;

        benchmarks_common::register_tables(&d_ctx, &data_dir).await?;

        // Query 15 has three queries in it, one creating the view, the second
        // executing, which we want to capture the output of, and the third
        // tearing down the view
        let plan = if query_id == "q15" {
            let queries: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            d_ctx.sql(queries[0]).await?.collect().await?;
            let df = d_ctx.sql(queries[1]).await?;
            let plan = df.create_physical_plan().await?;
            d_ctx.sql(queries[2]).await?.collect().await?;
            plan
        } else {
            let df = d_ctx.sql(&sql).await?;
            df.create_physical_plan().await?
        };

        collect(plan.clone(), d_ctx.task_ctx()).await?;
        let plan = rewrite_distributed_plan_with_metrics(plan, DistributedMetricsFormat::PerTask)?;
        println!("{}", display_plan_ascii(plan.as_ref(), true));
        Ok(())
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    // ensure_tpch_data initializes the TPCH data on disk.
    pub async fn ensure_tpch_data(sf: f64, parts: i32) -> std::path::PathBuf {
        let data_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("testdata/tpch/plan_sf{sf}"));
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
