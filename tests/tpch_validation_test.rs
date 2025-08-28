mod common;

#[cfg(all(feature = "integration", test))]
mod tests {
    use crate::common::{ensure_tpch_data, get_test_data_dir, get_test_tpch_query};
    use datafusion::error::DataFusionError;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{
        display_stage_graphviz, DistributedPhysicalOptimizerRule, DistributedSessionBuilderContext,
    };
    use futures::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

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

    async fn test_tpch_query(query_id: u8) -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context(2, build_state).await;
        run_tpch_query(ctx, query_id).await
    }

    async fn build_state(
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError> {
        let config = SessionConfig::new().with_target_partitions(3);

        let rule = DistributedPhysicalOptimizerRule::new().with_maximum_partitions_per_task(2);
        Ok(SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(ctx.runtime_env)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .build())
    }

    // test_non_distributed_consistency runs each TPC-H query twice - once in a distributed manner
    // and once in a non-distributed manner. For each query, it asserts that the results are identical.
    async fn run_tpch_query(ctx2: SessionContext, query_id: u8) -> Result<(), Box<dyn Error>> {
        ensure_tpch_data().await;
        let sql = get_test_tpch_query(query_id);

        // Context 1: Non-distributed execution.
        let config1 = SessionConfig::new().with_target_partitions(3);
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

            ctx2.register_parquet(
                table_name,
                query_path.to_string_lossy().as_ref(),
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await?;
        }

        // Query 15 has three queries in it, one creating the view, the second
        // executing, which we want to capture the output of, and the third
        // tearing down the view
        let (stream1, stream2) = if query_id == 15 {
            let queries: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            ctx1.sql(queries[0]).await?.collect().await?;
            ctx2.sql(queries[0]).await?.collect().await?;
            let df1 = ctx1.sql(queries[1]).await?;
            let df2 = ctx2.sql(queries[1]).await?;

            let stream1 = df1.execute_stream().await?;
            let stream2 = df2.execute_stream().await?;

            ctx1.sql(queries[2]).await?.collect().await?;
            ctx2.sql(queries[2]).await?.collect().await?;
            (stream1, stream2)
        } else {
            let stream1 = ctx1.sql(&sql).await?.execute_stream().await?;
            let stream2 = ctx2.sql(&sql).await?.execute_stream().await?;

            (stream1, stream2)
        };

        let batches1 = stream1.try_collect::<Vec<_>>().await?;
        let batches2 = stream2.try_collect::<Vec<_>>().await?;

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
