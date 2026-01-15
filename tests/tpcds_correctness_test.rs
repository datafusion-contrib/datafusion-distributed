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
    #[ignore = "Query q13 did not get distributed"]
    async fn test_tpcds_13() -> Result<()> {
        test_tpcds_query("q13").await
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "result sets were not equal: Internal error: Row content differs between result sets\nLeft set size: 100, Right set size: 100\n\nRows only in left (71 total):\n  NULL|NULL|NULL|NULL|674173362.51|155629\n  catalog|NULL|NULL|NULL|237410857.47|46322\n  catalog|1001001.00|NULL|NULL|1697729.02|347\n  catalog|1001001.00|1.00|NULL|855204.24|167\n  catalog|1001001.00|2.00|NULL|125167.22|24\n  catalog|1001001.00|3.00|NULL|198685.08|43\n  catalog|1001001.00|4.00|NULL|109585.97|31\n  catalog|1001001.00|5.00|NULL|59790.61|17\n  catalog|1001001.00|8.00|NULL|55768.46|13\n  catalog|1001001.00|8.00|7.00|28872.49|7\n  catalog|1001001.00|8.00|10.00|26895.97|6\n  catalog|1001001.00|9.00|NULL|30944.19|5\n  catalog|1001001.00|9.00|6.00|30944.19|5\n  catalog|1001001.00|11.00|NULL|82810.87|12\n  catalog|1001001.00|11.00|9.00|82810.87|12\n  catalog|1001001.00|12.00|NULL|38427.52|9\n  catalog|1001001.00|12.00|10.00|38427.52|9\n  catalog|1001001.00|15.00|NULL|112838.10|20\n  catalog|1001001.00|15.00|9.00|53508.79|7\n  catalog|1001001.00|15.00|10.00|59329.31|13\n  catalog|1001002.00|NULL|NULL|3527831.33|706\n  catalog|1001002.00|1.00|NULL|2673969.89|530\n  catalog|1001002.00|1.00|1.00|2673969.89|530\n  catalog|1001002.00|2.00|NULL|140831.91|29\n  catalog|1001002.00|2.00|1.00|140831.91|29\n  catalog|1001002.00|3.00|NULL|320175.87|67\n  catalog|1001002.00|3.00|1.00|320175.87|67\n  catalog|1001002.00|4.00|NULL|133287.96|21\n  catalog|1001002.00|4.00|1.00|133287.96|21\n  catalog|1001002.00|5.00|NULL|16606.90|9\n  catalog|1001002.00|5.00|1.00|16606.90|9\n  catalog|1001002.00|6.00|NULL|15133.01|4\n  catalog|1001002.00|6.00|1.00|15133.01|4\n  catalog|1001002.00|7.00|NULL|24471.26|10\n  catalog|1001002.00|7.00|1.00|24471.26|10\n  catalog|1001002.00|8.00|NULL|63773.05|12\n  catalog|1001002.00|8.00|1.00|63773.05|12\n  catalog|1001002.00|9.00|NULL|9167.19|3\n  catalog|1001002.00|9.00|1.00|9167.19|3\n  catalog|1001002.00|12.00|NULL|29108.42|7\n  catalog|1001002.00|12.00|1.00|29108.42|7\n  catalog|1001002.00|15.00|NULL|31143.45|6\n  catalog|1001002.00|15.00|1.00|31143.45|6\n  catalog|1001002.00|16.00|NULL|70162.42|8\n  catalog|1001002.00|16.00|1.00|70162.42|8\n  catalog|1002001.00|NULL|NULL|2114110.72|380\n  catalog|1002001.00|1.00|NULL|348693.97|55\n  catalog|1002001.00|1.00|1.00|76392.13|14\n  catalog|1002001.00|1.00|2.00|118394.33|21\n  catalog|1002001.00|1.00|4.00|29395.79|5\n  catalog|1002001.00|1.00|5.00|35541.97|4\n  catalog|1002001.00|1.00|6.00|26104.36|3\n  catalog|1002001.00|1.00|9.00|18793.97|4\n  catalog|1002001.00|1.00|10.00|44071.42|4\n  catalog|1002001.00|2.00|NULL|1233961.70|225\n  catalog|1002001.00|2.00|1.00|239511.02|51\n  catalog|1002001.00|2.00|2.00|147993.14|26\n  catalog|1002001.00|2.00|3.00|100086.93|17\n  catalog|1002001.00|2.00|4.00|53524.42|13\n  catalog|1002001.00|2.00|5.00|48494.06|10\n  catalog|1002001.00|2.00|6.00|142857.04|20\n  catalog|1002001.00|2.00|7.00|116557.98|16\n  catalog|1002001.00|2.00|8.00|92743.93|24\n  catalog|1002001.00|2.00|9.00|203943.99|38\n  catalog|1002001.00|2.00|10.00|88249.19|10\n  catalog|1002001.00|3.00|NULL|91054.32|17\n  catalog|1002001.00|3.00|2.00|25171.13|6\n  catalog|1002001.00|3.00|7.00|27766.70|3\n  catalog|1002001.00|3.00|8.00|38116.49|8\n  catalog|1002001.00|4.00|NULL|182427.69|32\n  catalog|1002001.00|4.00|1.00|66896.68|15\n\nRows only in right (71 total):\n  NULL|NULL|NULL|NULL|47788579.87|11068\n  NULL|NULL|NULL|NULL|46294358.79|10609\n  NULL|NULL|NULL|NULL|40499040.27|9321\n  NULL|NULL|NULL|NULL|37952602.75|8889\n  NULL|NULL|NULL|NULL|50256292.02|11540\n  NULL|NULL|NULL|NULL|27943616.98|6397\n  NULL|NULL|NULL|NULL|43114338.77|10000\n  NULL|NULL|NULL|NULL|56239021.04|13003\n  NULL|NULL|NULL|NULL|25682800.66|6012\n  NULL|NULL|NULL|NULL|38529122.81|8922\n  NULL|NULL|NULL|NULL|59222982.16|13528\n  NULL|NULL|NULL|NULL|48322926.86|11228\n  NULL|NULL|NULL|NULL|39166012.10|9010\n  NULL|NULL|NULL|NULL|32661391.26|7453\n  NULL|NULL|NULL|NULL|43315152.10|10008\n  NULL|NULL|NULL|NULL|37185124.07|8641\n  catalog|NULL|NULL|NULL|16671923.72|3228\n  catalog|NULL|NULL|NULL|16630833.01|3143\n  catalog|NULL|NULL|NULL|14038550.02|2798\n  catalog|NULL|NULL|NULL|13135427.84|2638\n  catalog|NULL|NULL|NULL|17604907.44|3399\n  catalog|NULL|NULL|NULL|10119873.49|1959\n  catalog|NULL|NULL|NULL|14698922.72|2919\n  catalog|NULL|NULL|NULL|19534422.18|3931\n  catalog|NULL|NULL|NULL|9075046.95|1756\n  catalog|NULL|NULL|NULL|13829338.20|2662\n  catalog|NULL|NULL|NULL|21769645.88|4087\n  catalog|NULL|NULL|NULL|16890254.59|3343\n  catalog|NULL|NULL|NULL|13897305.68|2680\n  catalog|NULL|NULL|NULL|11719010.15|2217\n  catalog|NULL|NULL|NULL|14773719.71|2947\n  catalog|NULL|NULL|NULL|13021675.89|2615\n  catalog|1001001.00|NULL|NULL|188446.33|41\n  catalog|1001001.00|NULL|NULL|53508.79|7\n  catalog|1001001.00|NULL|NULL|100105.28|23\n  catalog|1001001.00|NULL|NULL|114412.27|25\n  catalog|1001001.00|NULL|NULL|77231.70|15\n  catalog|1001001.00|NULL|NULL|174489.15|42\n  catalog|1001001.00|NULL|NULL|206490.30|38\n  catalog|1001001.00|NULL|NULL|45473.85|13\n  catalog|1001001.00|NULL|NULL|146344.47|27\n  catalog|1001001.00|NULL|NULL|152599.38|28\n  catalog|1001001.00|NULL|NULL|206412.37|36\n  catalog|1001001.00|NULL|NULL|119368.21|23\n  catalog|1001001.00|NULL|NULL|45014.15|12\n  catalog|1001001.00|NULL|NULL|50948.80|14\n  catalog|1001001.00|NULL|NULL|16883.97|3\n  catalog|1001001.00|1.00|NULL|100105.28|23\n  catalog|1001001.00|1.00|NULL|99985.35|21\n  catalog|1001001.00|1.00|NULL|107555.43|23\n  catalog|1001001.00|1.00|NULL|161349.39|29\n  catalog|1001001.00|1.00|NULL|146344.47|27\n  catalog|1001001.00|1.00|NULL|122521.31|25\n  catalog|1001001.00|1.00|NULL|77861.85|13\n  catalog|1001001.00|1.00|NULL|22597.19|3\n  catalog|1001001.00|1.00|NULL|16883.97|3\n  catalog|1001001.00|2.00|NULL|68565.38|14\n  catalog|1001001.00|2.00|NULL|43967.97|7\n  catalog|1001001.00|2.00|NULL|12633.87|3\n  catalog|1001001.00|3.00|NULL|60551.64|14\n  catalog|1001001.00|3.00|NULL|14426.92|4\n  catalog|1001001.00|3.00|NULL|36821.61|7\n  catalog|1001001.00|3.00|NULL|30078.07|3\n  catalog|1001001.00|3.00|NULL|28455.23|4\n  catalog|1001001.00|3.00|NULL|28351.61|11\n  catalog|1001001.00|4.00|NULL|47553.20|10\n  catalog|1001001.00|4.00|NULL|45473.85|13\n  catalog|1001001.00|4.00|NULL|16558.92|8\n  catalog|1001001.00|5.00|NULL|29678.50|5\n  catalog|1001001.00|5.00|NULL|30112.11|12\n  catalog|1001001.00|8.00|NULL|26895.97|6.\nThis issue was likely caused by a bug in DataFusion's code. Please help us to resolve this by filing a bug report in our issue tracker: https://github.com/apache/datafusion/issues"]
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
    #[ignore = "Query q48 did not get distributed"]
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
    #[ignore = "Query q62 did not get distributed"]
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
    #[ignore = "Query q91 did not get distributed"]
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
