mod common;

#[cfg(all(feature = "integration", test))]
mod tests {
    use crate::common::{ensure_tpch_data, get_test_data_dir, get_test_tpch_query};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;
    use std::error::Error;

    #[tokio::test]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        test_tpch_query(1).await
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        test_tpch_query(2).await
    }

    #[tokio::test]
    async fn test_tpch_3() -> Result<(), Box<dyn Error>> {
        test_tpch_query(3).await
    }

    #[tokio::test]
    async fn test_tpch_4() -> Result<(), Box<dyn Error>> {
        test_tpch_query(4).await
    }

    #[tokio::test]
    async fn test_tpch_5() -> Result<(), Box<dyn Error>> {
        test_tpch_query(5).await
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        test_tpch_query(6).await
    }

    #[tokio::test]
    async fn test_tpch_7() -> Result<(), Box<dyn Error>> {
        test_tpch_query(7).await
    }

    #[tokio::test]
    async fn test_tpch_8() -> Result<(), Box<dyn Error>> {
        test_tpch_query(8).await
    }

    #[tokio::test]
    async fn test_tpch_9() -> Result<(), Box<dyn Error>> {
        test_tpch_query(9).await
    }

    #[tokio::test]
    async fn test_tpch_10() -> Result<(), Box<dyn Error>> {
        test_tpch_query(10).await
    }

    #[tokio::test]
    async fn test_tpch_11() -> Result<(), Box<dyn Error>> {
        test_tpch_query(11).await
    }

    #[tokio::test]
    async fn test_tpch_12() -> Result<(), Box<dyn Error>> {
        test_tpch_query(12).await
    }

    #[tokio::test]
    async fn test_tpch_13() -> Result<(), Box<dyn Error>> {
        test_tpch_query(13).await
    }

    #[tokio::test]
    async fn test_tpch_14() -> Result<(), Box<dyn Error>> {
        test_tpch_query(14).await
    }

    #[tokio::test]
    #[ignore]
    // TODO: Support query 15?
    // Skip because it contains DDL statements not supported in single SQL execution
    async fn test_tpch_15() -> Result<(), Box<dyn Error>> {
        test_tpch_query(15).await
    }

    #[tokio::test]
    async fn test_tpch_16() -> Result<(), Box<dyn Error>> {
        test_tpch_query(16).await
    }

    #[tokio::test]
    async fn test_tpch_17() -> Result<(), Box<dyn Error>> {
        test_tpch_query(17).await
    }

    #[tokio::test]
    async fn test_tpch_18() -> Result<(), Box<dyn Error>> {
        test_tpch_query(18).await
    }

    #[tokio::test]
    async fn test_tpch_19() -> Result<(), Box<dyn Error>> {
        test_tpch_query(19).await
    }

    #[tokio::test]
    async fn test_tpch_20() -> Result<(), Box<dyn Error>> {
        test_tpch_query(20).await
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        test_tpch_query(21).await
    }

    #[tokio::test]
    async fn test_tpch_22() -> Result<(), Box<dyn Error>> {
        test_tpch_query(22).await
    }

    // test_non_distributed_consistency runs each TPC-H query twice - once in a distributed manner
    // and once in a non-distributed manner. For each query, it asserts that the results are identical.
    async fn test_tpch_query(query_id: u8) -> Result<(), Box<dyn Error>> {
        ensure_tpch_data().await;

        let sql = get_test_tpch_query(query_id);

        // Context 1: Non-distributed execution.
        let config1 = SessionConfig::new();
        let state1 = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config1)
            .build();
        let ctx1 = SessionContext::new_with_state(state1);

        // Register tables for first context
        for table_name in [
            "lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier",
        ] {
            let query_path = get_test_data_dir().join(format!("{}.parquet", table_name));
            ctx1.register_parquet(
                table_name,
                query_path.to_string_lossy().as_ref(),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await?;
        }

        let df1 = ctx1.sql(&sql).await?;
        let physical1 = df1.create_physical_plan().await?;

        let batches1 = execute_stream(physical1.clone(), ctx1.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        // Context 2: Distributed execution.
        // TODO: once distributed execution is working, we can enable distributed features here.
        let config2 = SessionConfig::new();
        // .with_target_partitions(3);
        let state2 = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config2)
            // .with_optimizer_rule(DistributedPhysicalOptimizerRule::default().with_maximum_partitions_per_task(4))
            .build();
        let ctx2 = SessionContext::new_with_state(state2);

        // Register tables for second context
        for table_name in [
            "lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier",
        ] {
            let query_path = get_test_data_dir().join(format!("{}.parquet", table_name));
            ctx2.register_parquet(
                table_name,
                query_path.to_string_lossy().as_ref(),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await?;
        }

        let df2 = ctx2.sql(&sql).await?;
        let physical2 = df2.create_physical_plan().await?;

        let batches2 = execute_stream(physical2.clone(), ctx2.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        let formatted1 = arrow::util::pretty::pretty_format_batches(&batches1)?;
        let formatted2 = arrow::util::pretty::pretty_format_batches(&batches2)?;

        assert_eq!(
            formatted1.to_string(),
            formatted2.to_string(),
            "Query {} results differ between executions",
            query_id
        );

        Ok(())
    }
}
