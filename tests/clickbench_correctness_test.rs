#[cfg(all(feature = "integration", feature = "clickbench", test))]
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
    use datafusion_distributed::test_utils::{benchmarks_common, clickbench};
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, display_plan_ascii,
    };
    use std::ops::Range;
    use std::path::Path;
    use std::sync::Arc;
    use tokio::sync::OnceCell;

    const NUM_WORKERS: usize = 4;
    const FILES_PER_TASK: usize = 2;
    const CARDINALITY_TASK_COUNT_FACTOR: f64 = 2.0;
    const FILE_RANGE: Range<usize> = 0..3;

    #[tokio::test]
    #[ignore = "Query 0 did not get distributed"]
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
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 1, Right set size: 1\n\nRows only in left (1 total):\n  2533767602294735360.00\n\nRows only in right (1 total):\n  2533767602294735872.00.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
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
    #[ignore = "Query 6 did not get distributed"]
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
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (10 total):\n  3219866204653196665||4\n  3220056705148678697||11\n  3221898002592879542||1\n  3223026783585713477||23\n  3223839745005575457||116\n  3223839745005575457|d0bcd0bed0b6d0bdd0be20d0bbd0b820d0b220d0bad180d0bed0bad0bed0b4d0b8d180d0bed0b2d0b5d0bbd18cd18820d0b1d180d0bed0b4d18b20d0bdd0b020d181d182d0bed0bbd18b20d0b2d0be20d0b2d0bbd0b0d0b4d0b8d0b2d0bed181d182d0bed0ba20d0b2d0b2d0be|1\n  3223949769615485893||1\n  3226415756450197918||24\n  3226664959488084815||62\n  3227160743723019373||71\n\nRows only in right (10 total):\n  700182585509527889||2\n  724127359630680276|d0b8d0b3d180d18b20d0b820d181d0b5d0b3d0bed0b4d0bdd18f3f|1\n  766120398574852544||1\n  766739966065297239||1\n  783205612738304865||3\n  797289180007803204||2\n  804968013253615745||1\n  830548852254311605||1\n  849024737642146119||1\n  849169469997862534||1.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
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
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (5 total):\n  d181d0bbd0b0d0b2d0bbd18fd182d18c20d0bfd0bed180d0bed0b4d0b8d182d181d18f20d0bed182d0b5d0bbd0b8203230313320d181d0bcd0bed182d180d0b5d182d18c|687474703a253246253246766b2e636f6d2e75612f676f6f676c652d6a61726b6f76736b6179612d4c697065636b64|1\n  d0b1d0b0d0bdd0bad0bed0bcd0b0d182d0b5d180d0b8d0b0d0bbd18b20d181d0bcd0bed182d180d0b5d182d18c|687474703a2f2f6f72656e627572672e6972722e72752532466b7572746b692532462532467777772e676f6f676c652e72752f6d617a64612d332d6b6f6d6e2d6b762d4b617a616e2e74757475746f72736b2f64657461696c|1\n  d0bcd0bed0bdd0b8d182d18c20d0bad0b0d0bad0bed0b520d0bed0b7d0b5d180d0b0|687474703a2f2f6175746f2e7269612e75612f6175746f5f69643d30266f726465723d46616c7365266d696e707269782e72752f6b617465676f726979612f767369652d646c69612d647275676f652f6d61746572696e7374766f2f676f6f676c652d706f6c697331343334343532|1\n  d181d0bad0b0d187d0b0d182d18c20d0b4d0b5d0bdd0b5d0b320d181d183d180d0b3d183d182|687474703a2f2f7469656e736b6169612d6d6f64612d627269657469656c6b612d6b6f736b6f76736b2f64657461696c2e676f6f676c65|1\n  d0b220d0b0d0b2d0b3d183d181d1822032343720d0b3d180d183d181d182d0b8d0bcd0bed188d0bad0b020d0bdd0b020d0bad180d0b8d181d182d180d0b0d182|687474703a2f2f7469656e736b6169612d6d6f64612d627269756b692f676f6f676c652e72752f7e61706f6b2e72752f635f312d755f313138383839352c39373536|1\n\nRows only in right (5 total):\n  d0bcd0bed0b4d0b5d0bad18120d183d0bbd0b8d186d0b5d0bdd0b7d0b8d0bdd0bed0b2d0b020d0b3d0bed0b2d18fd0b4d0b8d0bdd0b0|687474703a2f2f73616d6172612e6972722e72752f636174616c6f675f676f6f676c652d636865726e796a2d393233353636363635372f3f64617465|1\n  d0bbd0b0d0b2d0bfd0bbd0b0d0bdd188d0b5d182d0bdd0b8d18520d183d181d0bbd0bed0b2d0b0d0bcd0b820d0b2d181d0b520d181d0b5d180d0b8d0b820d0b4d0b0d182d0b020d186d0b5d0bcd0b5d0bdd0b8|687474703a2f2f73616d6172612e6972722e72752f636174616c6f675f676f6f676c654d425225323661642533443930253236707a|1\n  d0bad0b0d0ba20d0bfd180d0bed0b4d0b0d0bcd0b820d0b4d0bbd18f20d0b4d0b5d0b2d183d188d0bad0b8|687474703a253246253246777777772e626f6e707269782e7275253235326625323532663737363925323532663131303931392d6c65766f652d676f6f676c652d7368746f72792e72752f666f72756d2f666f72756d2e6d617465722e72752f6461696c792f63616c63756c61746f72|1\n  d0b6d0b0d180d0b5d0bdd18cd18f20d0b32ed181d183d180d0bed0b2d0b0d0bdd0b8d0b520d0b2d0bed180d0bed0bdd0b5d0b6d181d0bad0b0d18f20d0bed0b1d0bbd0b0d181d182d0bed0bfd180d0b8d0bbd0b520d0bfd0bed181d0bbd0b5d0b4d0bdd0b8d0b520d0bad0bed181d18b|687474703a2f2f756b7261696e627572672f65636f2d6d6c656b2f65636f6e646172792f73686f77746f7069632e7068703f69643d3436333837362e68746d6c3f69643d32303634313333363631253246676f6f676c652d4170706c655765624b69742532463533372e333620284b48544d4c2c206c696b65|1\n  d180d0b8d0be20d0bdd0b020d0bad0b0d180d182d0bed187d0bdd0b8d186d0b020d181d0bcd0bed182d180d0b5d182d18c20d0bed0bdd0bbd0b0d0b9d0bd|687474703a2f2f73616d6172612e6972722e72752f636174616c6f675f676f6f676c654d425225323661642533443930253236707a|1.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_21() -> Result<()> {
        test_clickbench_query("q21").await
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (1 total):\n  d0bad0b0d0bad0bed0b920d0bfd0bbd0bed189d0b0d0b4d0bad0b8d0bcd0b820d0b4d0bed181d182d0b0d0b2d0bad0b8|687474703a253246253246766b2e636f6d2f696672616d652d6f77612e68746d6c3f313d31266369643d353737266f6b693d31266f705f63617465676f72795f69645d3d332673656c656374|d092d0b0d0bad0b0d0bdd181d0b8d18f20d091d0a0d090d09ad090d09d20d090d09dd094d0a0d095d0a1202d20d0bfd0bed0bfd0b0d0bbd0b820d0bad183d0bfd0b8d182d18c20d0b4d0bed0bcd0bed0b5d187d0bdd18bd0b520d188d0bad0b0d184d0b020476f6f676c652e636f6d203a3a20d0bad0bed182d182d0b5d0bad181d1822c20d091d183d180d18fd182d0bdd0b8d0bad0b820d0b4d0bbd18f20d0bfd0b5d187d18c20d0bcd0b5d0b1d0b5d0bbd18cd0b520d0b4d0bbd18f20d0b4d0b5d0b2d183d188d0bad0b0|5|1\n\nRows only in right (1 total):\n  d0bad0bed0bfd182d0b8d0bcd0b8d0bad0b2d0b8d0b4d18b20d18ed180d0b8d0b920d0bfd0bed181d0bbd0b5d0b4d0bdd18fd18f|68747470733a2f2f70726f64756b747925324670756c6f76652e72752f626f6f6b6c79617474696f6e2d7761722d73696e696a2d393430343139342c3936323435332f666f746f|d09bd0b5d0b3d0bad0be20d0bdd0b020d183d187d0b0d181d182d0bdd18bd0b520d183d187d0b0d181d182d0bdd0b8d0bad0bed0b22e2c20d0a6d0b5d0bdd18b202d20d0a1d182d0b8d0bbd18cd0bdd0b0d18f20d0bfd0b0d180d0bdd0b5d0bc2e20d0a1d0b0d0b3d0b0d0bdd180d0bed0b320d0b4d0bed0b3d0b0d0b4d0b5d0bdd0b8d18f203a20d0a2d183d180d186d0b8d0b82c20d0bad183d0bfd0b8d182d18c20d18320313020d0b4d0bdd0b520d0bad0bed0bbd18cd0bdd18bd0b520d0bcd0b0d188d0b8d0bdd0bad0b820d0bdd0b520d0bfd180d0b5d0b4d181d182d0b0d0b2d0bad0b8202d20d09dd0bed0b2d0b0d18f20d18120d0b8d0b7d0b1d0b8d0b5d0bdd0b8d0b520d181d0bfd180d0bed0b4d0b0d0b6d0b03a20d0bad0bed182d18fd182d0b0203230313420d0b32ed0b22e20d0a6d0b5d0bdd0b03a2034373530302d313045434f30363020e28093202d2d2d2d2d2d2d2d20d0bad183d0bfd0b8d182d18c20d0bad0b2d0b0d180d182d0b8d180d18320d09ed180d0b5d0bdd0b1d183d180d0b32028d0a0d0bed181d181d0b8d0b82047616c616e7472617820466c616d696c6961646120476f6f676c652c204ed0be20313820d184d0bed182d0bed0bad0bed0bdd0b2d0b5d180d0ba20d0a1d183d0bfd0b5d18020d09ad0b0d180d0b4d0b8d0b3d0b0d0bd|5|1.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_22() -> Result<()> {
        test_clickbench_query("q22").await
    }

    #[tokio::test]
    async fn test_clickbench_23() -> Result<()> {
        test_clickbench_query("q23").await
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (1 total):\n  d0b2d181d0bfd0bed0bcd0bdd0b8d182d18c20d181d0bed0bbd0bdd0b5d0bdd0b8d0b520d0b1d0b0d0bdd0bad0b020d0bbd0b0d0b420d184d0b8d0bbd18cd0bc\n\nRows only in right (1 total):\n  d0bed182d0b2d0bed0b4d0b020d0b4d0bbd18f20d0bfd0b8d180d0bed0b6d0bad0b820d0bbd0b5d187d0b5d0bdd0bdd18b20d0b2d181d0b520d181d0b5d180d196d197.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
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
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (10 total):\n  8673025726158767406|1264438551|1|0|1990.00\n  5320052218057629211|-1703087277|1|0|1996.00\n  6244273852606083750|1554672832|1|0|1638.00\n  8628753750962053665|1215278356|1|0|1087.00\n  7035318163404387241|1326714320|1|0|1638.00\n  8431857775494210873|1237512945|1|0|1996.00\n  5110752526539992124|37611695|1|0|1917.00\n  8986794334343068049|1860752926|1|0|1638.00\n  8044147848299485837|1382122372|1|0|1368.00\n  7936057634954670727|1897481896|1|0|1638.00\n\nRows only in right (10 total):\n  5132615111782210132|-50313020|1|0|1368.00\n  5783789691451717551|-1310327384|1|0|1638.00\n  5756260993772351383|1484317883|1|0|375.00\n  7739310142000732364|991864113|1|0|1368.00\n  7593472904893539271|-151291403|1|0|1087.00\n  6339599967989898410|1543815587|1|0|1638.00\n  7794346560421945218|1645556180|1|0|1368.00\n  6112645108657361792|593586188|1|0|1638.00\n  6675910710751922756|-816379256|1|0|1368.00\n  5802727636196431835|1986422271|1|0|1996.00.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
    async fn test_clickbench_31() -> Result<()> {
        test_clickbench_query("q31").await
    }

    #[tokio::test]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 10, Right set size: 10\n\nRows only in left (10 total):\n  7643059318918524417|1767085700|1|0|0.00\n  5437163248266133938|-1465369615|1|0|0.00\n  9142541582422390102|-1465369615|1|0|0.00\n  8438994503411842126|-1465369615|1|0|0.00\n  7362096505818029859|-565678477|1|0|0.00\n  4928022308880516715|1699955284|1|0|0.00\n  5269769817689282522|1699955284|1|0|0.00\n  9081648050908046886|1699955284|1|0|0.00\n  6824181869275536503|1699955284|1|0|0.00\n  6905712404475757487|1552811156|1|0|0.00\n\nRows only in right (10 total):\n  6967277596165459879|-941091661|1|0|1368.00\n  5796237228224217668|-1310327384|1|0|1638.00\n  7218628137278606666|1511490240|1|0|1638.00\n  8314760197723815280|1566105210|1|0|1996.00\n  7053263954762394007|757778490|1|0|339.00\n  6283334114093174531|1216031795|1|0|1368.00\n  8818295356247036741|83042182|1|0|1638.00\n  6620528864937282562|-862894777|1|0|1996.00\n  8466121050002905379|83042182|1|0|1638.00\n  7554844936512227411|-1746904856|1|0|1368.00.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
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
    async fn test_clickbench_38() -> Result<()> {
        test_clickbench_query("q38").await
    }

    #[tokio::test]
    async fn test_clickbench_39() -> Result<()> {
        test_clickbench_query("q39").await
    }

    #[tokio::test]
    async fn test_clickbench_40() -> Result<()> {
        test_clickbench_query("q40").await
    }

    #[tokio::test]
    async fn test_clickbench_41() -> Result<()> {
        test_clickbench_query("q41").await
    }

    #[tokio::test]
    #[ignore = "ordering mismatch: expected ordering: Some(LexOrdering { exprs: [PhysicalSortExpr { expr: ScalarFunctionExpr { fun: \"<FUNC>\", name: \"date_trunc\", args: [Literal { value: Utf8(\"minute\"), field: Field { name: \"lit\", data_type: Utf8 } }, Column { name: \"m\", index: 0 }], return_field: Field { name: \"date_trunc\", data_type: Timestamp(Second, None), nullable: true } }, options: SortOptions { descending: false, nulls_first: false } }], set: {ScalarFunctionExpr { fun: \"<FUNC>\", name: \"date_trunc\", args: [Literal { value: Utf8(\"minute\"), field: Field { name: \"lit\", data_type: Utf8 } }, Column { name: \"m\", index: 0 }], return_field: Field { name: \"date_trunc\", data_type: Timestamp(Second, None), nullable: true } }} }), actual ordering: Some(LexOrdering { exprs: [PhysicalSortExpr { expr: ScalarFunctionExpr { fun: \"<FUNC>\", name: \"date_trunc\", args: [Literal { value: Utf8(\"minute\"), field: Field { name: \"lit\", data_type: Utf8 } }, Column { name: \"m\", index: 0 }], return_field: Field { name: \"date_trunc\", data_type: Timestamp(Second, None), nullable: true } }, options: SortOptions { descending: false, nulls_first: false } }], set: {ScalarFunctionExpr { fun: \"<FUNC>\", name: \"date_trunc\", args: [Literal { value: Utf8(\"minute\"), field: Field { name: \"lit\", data_type: Utf8 } }, Column { name: \"m\", index: 0 }], return_field: Field { name: \"date_trunc\", data_type: Timestamp(Second, None), nullable: true } }} })"]
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
        let (d_ctx, _guard, _) = start_localhost_context(NUM_WORKERS, DefaultSessionBuilder).await;
        let d_ctx = d_ctx
            .with_distributed_files_per_task(FILES_PER_TASK)?
            .with_distributed_cardinality_effect_task_scale_factor(CARDINALITY_TASK_COUNT_FACTOR)?
            .with_distributed_broadcast_joins(true)?;

        benchmarks_common::register_tables(&s_ctx, &data_dir).await?;
        benchmarks_common::register_tables(&d_ctx, &data_dir).await?;

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
