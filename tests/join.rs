#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::util::pretty;
    use datafusion::{
        assert_batches_eq,
        physical_plan::{collect, displayable},
        prelude::{ParquetReadOptions, SessionContext},
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, display_plan_ascii,
        test_utils::localhost::start_localhost_context,
    };

    #[tokio::test]
    async fn test_join_distributed() -> Result<(), Box<dyn std::error::Error>> {

        let query = "SELECT * FROM dim JOIN fact ON dim.key = fact.key ORDER BY dim.key, dim.col1, fact.col1";

        // Execute the query using single node datafusion.
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "dim",
            "testdata/join/parquet/dim",
            ParquetReadOptions::new(),
        )
        .await?;
        ctx.register_parquet(
            "fact",
            "testdata/join/parquet/fact",
            ParquetReadOptions::new(),
        )
        .await?;

        let df = ctx.sql(query).await?;

        let (state, logical_plan) = df.into_parts();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        // println!("\n——————— PHYSICAL PLAN ———————\n");
        println!("{}", displayable(physical_plan.as_ref()).indent(true));

        let non_distributed_result = collect(physical_plan, state.task_ctx()).await?;
        pretty::print_batches(&non_distributed_result)?;

        // Execute the query using distributed datafusion.
        let (distributed_ctx, _guard) = start_localhost_context(4, DefaultSessionBuilder).await;
        distributed_ctx
            .register_parquet(
                "dim",
                "testdata/join/parquet/dim",
                ParquetReadOptions::new(),
            )
            .await?;
        distributed_ctx
            .register_parquet(
                "fact",
                "testdata/join/parquet/fact",
                ParquetReadOptions::new(),
            )
            .await?;

        distributed_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        distributed_ctx
            .state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;

        let df = distributed_ctx.sql(query).await?;

        let (state, logical_plan) = df.into_parts();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        println!("\n——————— DISTRIBUTED PLAN ———————\n");
        println!("{}", display_plan_ascii(physical_plan.as_ref(), false));

        let distributed_result = collect(physical_plan, state.task_ctx()).await?;
        pretty::print_batches(&distributed_result)?;

        // Compare single-node and distributed results.
        assert_eq!(non_distributed_result, distributed_result);

        Ok(())
    }
}

