#[cfg(all(feature = "integration", feature = "clickbench", test))]
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
        test_clickbench_query("q0").await
    }

    #[tokio::test]
    async fn test_clickbench_1() -> Result<()> {
        test_clickbench_query("q1").await
    }

    #[tokio::test]
    async fn test_clickbench_2() -> Result<()> {
        test_clickbench_query("q2").await
    }

    #[tokio::test]
    async fn test_clickbench_3() -> Result<()> {
        test_clickbench_query("q3").await
    }

    #[tokio::test]
    async fn test_clickbench_4() -> Result<()> {
        test_clickbench_query("q4").await
    }

    #[tokio::test]
    async fn test_clickbench_5() -> Result<()> {
        test_clickbench_query("q5").await
    }

    #[tokio::test]
    #[ignore = "Query 6 did not get distributed.The planner correctly chooses a single-task plan because of parquet statistics."]
    async fn test_clickbench_6() -> Result<()> {
        test_clickbench_query("q6").await
    }

    #[tokio::test]
    async fn test_clickbench_7() -> Result<()> {
        test_clickbench_query("q7").await
    }

    #[tokio::test]
    async fn test_clickbench_8() -> Result<()> {
        test_clickbench_query("q8").await
    }

    #[tokio::test]
    async fn test_clickbench_9() -> Result<()> {
        test_clickbench_query("q9").await
    }

    #[tokio::test]
    async fn test_clickbench_10() -> Result<()> {
        test_clickbench_query("q10").await
    }

    #[tokio::test]
    async fn test_clickbench_11() -> Result<()> {
        test_clickbench_query("q11").await
    }

    #[tokio::test]
    async fn test_clickbench_12() -> Result<()> {
        test_clickbench_query("q12").await
    }

    #[tokio::test]
    async fn test_clickbench_13() -> Result<()> {
        test_clickbench_query("q13").await
    }

    #[tokio::test]
    async fn test_clickbench_14() -> Result<()> {
        test_clickbench_query("q14").await
    }

    #[tokio::test]
    async fn test_clickbench_15() -> Result<()> {
        test_clickbench_query("q15").await
    }

    #[tokio::test]
    async fn test_clickbench_16() -> Result<()> {
        test_clickbench_query("q16").await
    }

    #[tokio::test]
    #[ignore = "query produces non-deterministic results, cannot compare directly"]
    async fn test_clickbench_17() -> Result<()> {
        test_clickbench_query("q17").await
    }

    #[tokio::test]
    async fn test_clickbench_18() -> Result<()> {
        test_clickbench_query("q18").await
    }

    #[tokio::test]
    async fn test_clickbench_19() -> Result<()> {
        test_clickbench_query("q19").await
    }

    #[tokio::test]
    async fn test_clickbench_20() -> Result<()> {
        test_clickbench_query("q20").await
    }

    #[tokio::test]
    #[ignore = "query produces non-deterministic results, cannot compare directly"]
    async fn test_clickbench_21() -> Result<()> {
        test_clickbench_query("q21").await
    }

    #[tokio::test]
    #[ignore = "query produces non-deterministic results, cannot compare directly"]
    async fn test_clickbench_22() -> Result<()> {
        test_clickbench_query("q22").await
    }

    #[tokio::test]
    async fn test_clickbench_23() -> Result<()> {
        test_clickbench_query("q23").await
    }

    #[tokio::test]
    #[ignore = "query produces non-deterministic results, cannot compare directly"]
    async fn test_clickbench_24() -> Result<()> {
        test_clickbench_query("q24").await
    }

    #[tokio::test]
    async fn test_clickbench_25() -> Result<()> {
        test_clickbench_query("q25").await
    }

    #[tokio::test]
    async fn test_clickbench_26() -> Result<()> {
        test_clickbench_query("q26").await
    }

    #[tokio::test]
    async fn test_clickbench_27() -> Result<()> {
        test_clickbench_query("q27").await
    }

    #[tokio::test]
    async fn test_clickbench_28() -> Result<()> {
        test_clickbench_query("q28").await
    }

    #[tokio::test]
    async fn test_clickbench_29() -> Result<()> {
        test_clickbench_query("q29").await
    }

    #[tokio::test]
    async fn test_clickbench_30() -> Result<()> {
        test_clickbench_query("q30").await
    }

    #[tokio::test]
    #[ignore = "query produces non-deterministic results, cannot compare directly"]
    async fn test_clickbench_31() -> Result<()> {
        test_clickbench_query("q31").await
    }

    #[tokio::test]
    #[ignore = "query produces non-deterministic results, cannot compare directly"]
    async fn test_clickbench_32() -> Result<()> {
        test_clickbench_query("q32").await
    }

    #[tokio::test]
    async fn test_clickbench_33() -> Result<()> {
        test_clickbench_query("q33").await
    }

    #[tokio::test]
    async fn test_clickbench_34() -> Result<()> {
        test_clickbench_query("q34").await
    }

    #[tokio::test]
    async fn test_clickbench_35() -> Result<()> {
        test_clickbench_query("q35").await
    }

    #[tokio::test]
    async fn test_clickbench_36() -> Result<()> {
        test_clickbench_query("q36").await
    }

    #[tokio::test]
    async fn test_clickbench_37() -> Result<()> {
        test_clickbench_query("q37").await
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (10 total):\n  687474703a2f2f766964656f2e79616e6465782e72752f63617465676f72795f6e616d653d312664616d6167652d70617065722f746f77657246726f6d3d266675656c5261746546726f6d|2\n  6874747025334125323532663131303931312e68746d6c|2\n  687474703a2f2f766964656f2e79616e6465782e7068703f723d31373833372f70686f746f3d31266369643d353232333137383739332e68746d6c3f313d31266369643d353737266f705f70726f64756374732f696e6465782e72752f756c69747a|2\n  687474703a2f2f7374616c6b65722d7075622d32303038373839383637353439342c3936303934382f23706167655f7479706525334430253236707a25334430253236726c6575726c2533442f2f61642e616472697665722e72752f70686f746f3d302669735f686f743d30266175746f5f69643d353737266f6b693d31266f705f70726f64616d2d312d6b6f6d6e2d6b76617274692d6d2e72752f616c6c70726963655f646f3d323030352670766e6f3d322665766c673d56432c313b564c2c3136333b49432c3231353b38343d3238313232302532366e6964|2\n  687474703a2f2f746f6c79617474697265732f3f69643d323832302f23726576696577|2\n  687474703a2f2f7374616c6b65722d7075622d32303038373839383637353439342c3936303934382f23706167655f7479706525334430253236707a25334430253236726c6575726c2533442f2f61642e616472697665722e72752f70686f746f3d302669735f686f743d30266175746f5f69643d353737266f6b693d31266f705f70726f64616d2d312d6b6f6d6e2d6b76617274692d6d2e72752f616c6c70726f636573732f6f746865722f7265736964656e742f312d6b6f6d6e61746e6f5f6b6f746f76696d2d646f6d612e72752f736561726368|2\n  687474703a2f2f74766964692e72752f7265616c2d6573746174652f61706172746e6572732f73746174653d30266d6f64656c3d3026417265613d32266c73744d61726b3d6368652662616c616e6979752e68746d6c3f73733d6a617661742f556e69765f2d5f5453534b41|2\n  687474703a2f2f736d6f6c656e736b6169612d6d6f64612d7a6869656e736174736969|2\n  687474703a2f2f766964656f2e79616e6465782e72752f62616e6e6574|2\n  687474703a2f2f6a6f62696e6d6f73636f772f64657461696c2f35323932373137353432333732393032352f3f536561726368|2\n\nRows only in right (10 total):\n  687474703a2f2f77696c64626572726965732e61737078236c6f636174696f6e2f67726f75705f636f645f31733d353326627574746f5f726570616972733d3026776974685f70686f746f2f36393336332f322366|2\n  687474703a2f2f7374616c6b65722d7075622d32303038373839383637353439342c3936303934382f23706167655f747970652533443236303131373135323333372673706e3d313339352c393435353938392e79612e72752f776f726c642f70686f746f3d312673746e616d653dd09fd0bed0bad183d0bfd0bad0b820d0b220d0bcd0bed181d0bad0b2d0b526796e3d3126696e7075745f77686f323d3126696e7075745f616374696f6e2f766163616e636965732f73686f702e7370622e7361756e613132332f233234736d692e6f72672e72752f727a6e2e6e65742532463533372e33362668653d3736382677693d3133363626627574746f|2\n  687474703a2f2f7374616c6b65722d7075622d32303038373839383637353439342c3936303934382f23706167655f747970652533443236303131373135323333372673706e3d313339352c39343733353639343834353535322f70616765253344302532366e6964253344313538313937253236616e626965746572735f62756c61746f722f74776f5f6368616d626572732f2370616765322f236f7665726c616e64|2\n  687474703a2f2f766964656f2e79616e6465782e72752f73746174653d302673616c6564506172616d7325334472686f7374253344343325323673696425334430|2\n  687474703a2f2f766964656f2e79616e6465782e72752f766163616e63696573|2\n  687474703a2f2f736c6f766172692e79616e6465782e727526707669643d31266d733d31|2\n  687474703a2f2f6166697368612e79616e6465782e75612f7365617263683d3026636f6c6f723d3026617574683d31333733333649507073334d2677686572653d616c6c26746578743d7468696e20467265654d486b6c632677686572653d616c6c267372743d30266175746f2e7269612e75612f7365617263682f7461626c652f766965775f747970655f69643d3133373333302f3234392f3f5365617263682f61625f6469737472696368696576655f706172616d7325334472686f7374253344343030253236736964253344313538313937253236616425334430253236726c6575726c253344253236436f6d7050617468|2\n  687474703a2f2f6d6170732372755f355f72755f3232375f72755f333633302673746174652f61706172746d656e74732d73616c652f7365636f6e646172792f7069632f38393339352663683d5554462d382673463d31312c372c3730302664743d313030303826706f5f796572733d323030342d672d762d70726f6d6f25334468747470|2\n  687474703a2f2f7374616c6b65722d7075622d32303038373839383637353439342c3936303934382f23706167655f747970652533443236303131373135323333372673706e3d313339352c3934353933303162643936392f6375727265322e68746d6c5f706172616d3d30266472697665722e72752f6d6f73636f772f64657461696c732f333938313632267265745f426c61636b5f6c6973743d3026766965775f63617465645f636172|2\n  687474703a2f2f74617474696f6e2f727573726164696f7265636f72642e72752f31325f73616d5f6e61706973616c5f706172746d656e74237864|2.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_38() -> Result<()> {
        test_clickbench_query("q38").await
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (9 total):\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323439313732342f3f62756e646c6572732f7365617263683f74657874|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6b6170757374612d616476657274323637373538372e3132353030266d6174636865736b69792d725f6e2f746969643d30266c6173745f6175746f|13\n  -1|0|0|687474703a2f2f6b696e6f706f69736b2e72752f79616e6465782e72752f696e6465782e72752f3f61|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6c656e697961373737373239342c393338333033313330|13\n  -1|0|0|687474703a2f2f6b696e6f706f69736b2e72752f676f6c64|687474703a2f2f6972722e72752f696e74726f6c75785f70616765352f322f706167655479706549643d302665786368616e6765762e7068703f723d31383131373236|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323030362f6d616b756d69726f73686f6f7762697a2f646f776e253246686f6c6f64696c6e696b2e72752f37362f7e382f|687474703a2f2f6972722e72752f696e6465782e706870|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323439313732342f3f62756e646c6572732f7365617263683f74657874|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6b6170757374612d61647665727432363231372e312668653d31302663617465676f72793d61727469636c652f363937373136266f705f63617465676f7279|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323030362f6d616b756d|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e6f2d732d67726967657261746f722f70616765313d26696e7075745f616765313d|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323439313732342f3f62756e646c6572732f7365617263683f74657874|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6b6170757374612d61647665727432373331302e68746d6c3f69643d362670763d323226696e7075745f77686f323d3126696e7075745f636f756e7470616765|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323030362f6d616b756d69726f73686f6f7762697a2f646f776e253246686f6c6f64696c6e696b2e72752f373635352674657874|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6c656e697961373737373239342c393338333033313330|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d676f726f642f7365617263683f703d37266f70726e643d393930322e6a706726696d675f75726c3d687474703a2f2f79616e64736561726368|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6b6170757374612d616476657274323732376f62486d73334d2677686572653d616c6c2666696c6d49643d696d706f7274697a616e736b|13\n\nRows only in right (9 total):\n  -1|0|0|687474703a2f2f6b696e6f706f69736b2e72752f3f7374617465|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d393634392e68746d6c5f706172616d7325334472686f737425334461642e616472697665722e7275|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323030362f6d616b756d69726f73686f6f7762697a2f646f776e253246686f6c6f64696c6e696b2e72752f373635313535|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6c656e697961373737373239342c393338333033313330|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d6b617465676f726979612f7a6869656e736d65642e72752f726563697065732f7365617263683f6c723d32383526756c6f67696e3d69632d73656c662f6c6f67696e3d72656b6d32303132266270703d3132266d61696e2e617370783f736f72743d70726963655f6d617035|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6b6170757374612d616476657274323631363233343437343133333339266d6f64656c3d32333139|13\n  0|0|0||687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d323031312f3433353937|13\n  0|0|0||687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6b6170757374612d61647665727432373430393734253236707a2533443025323661725f736c6963656964253344373238253236686569676874|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323030362f6d616b756d69726f73746f76612e72752f612d6d7970726f66696c652533446765746e65773d26747970653d73696d616765|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6c656e697961373737373239342c393338333033313330|13\n  -1|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323439313732342f3f62756e646c6572732f7365617263683f74657874|687474703a2f2f6972722e72752f696e6465782e7068703f73686f77616c62756d2f6c6f67696e2d6b6170757374612d61647665727432363133352f3f706167653d31266675656c5261746546726f6d3d26656e67696e65566f6c756d6546726f6d|13\n  5|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323030362f6d616b756d69726f73746f76612e72752f63756c74757265732f34373637392f617274732f616e73776572737765622e7275|687474703a2f2f656b627572672e6972722e727525324670756c6f7665706c616e6574|13\n  5|0|0|687474703a2f2f73746174653d31393934353230362f666f746f2d342f6c6f67696e2d323030362f6d616e6761|687474703a2f2f6d796c6f7665706c616e65742e72752f696e6465782e72752f726567697374726963743d333231392673743d313023|13.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_39() -> Result<()> {
        test_clickbench_query("q39").await
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (1 total):\n  -4099353604574108490|2013-07-15|21\n\nRows only in right (1 total):\n  5588583429439052564|2013-07-15|21.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_40() -> Result<()> {
        test_clickbench_query("q40").await
    }

    #[tokio::test]
    async fn test_clickbench_41() -> Result<()> {
        test_clickbench_query("q41").await
    }

    #[tokio::test]
    #[ignore = "Ordering mismatch on `date_trunc('minute', ...)`: `compare_ordering` reports `LexOrdering` inequality even thought printed results match"]
    async fn test_clickbench_42() -> Result<()> {
        test_clickbench_query("q42").await
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

    async fn test_clickbench_query(query_id: &str) -> Result<()> {
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
        if !d_plan.is::<DistributedExec>() {
            return plan_err!("Query {query_id} did not get distributed");
        }
        let display = display_plan_ascii(d_plan.as_ref(), false);
        println!("Query {query_id}:\n{display}");

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
