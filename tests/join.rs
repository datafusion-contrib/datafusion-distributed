#[cfg(test)]
mod tests {
    use arrow::{datatypes::DataType, util::pretty};
    use datafusion::{
        physical_plan::{collect, displayable},
        prelude::{ParquetReadOptions, SessionContext},
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, display_plan_ascii, test_utils::localhost::start_localhost_context,
    };

    #[tokio::test]
    async fn test_join_distributed() -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
        SELECT 
                f.f_dkey,
                f.timestamp,
                f.value,
                d.env,
                d.service,
                d.host
            FROM dim d
            INNER JOIN fact f ON d.d_dkey = f.f_dkey
            ORDER BY f.f_dkey, f.timestamp
        "#;

        // Execute the query using single node datafusion.
        let ctx = SessionContext::new();

        // Register hive-style partitioning for the dim table.
        let dim_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
        ctx.register_parquet("dim", "testdata/join/parquet/dim", dim_options)
            .await?;

        // Register hive-style partitioning for the fact table.
        let fact_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("f_dkey".to_string(), DataType::Utf8)]);
        ctx.register_parquet("fact", "testdata/join/parquet/fact", fact_options)
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
        // Register hive-style partitioning for the dim table.
        let dim_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
        distributed_ctx
            .register_parquet("dim", "testdata/join/parquet/dim", dim_options)
            .await?;

        // Register hive-style partitioning for the fact table.
        let fact_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("f_dkey".to_string(), DataType::Utf8)]);
        distributed_ctx
            .register_parquet("fact", "testdata/join/parquet/fact", fact_options)
            .await?;

        set_optimizer_settings(&distributed_ctx);

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

    fn set_optimizer_settings(ctx: &SessionContext) {
        // Ensure that we always use a partitioned hash join.
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = 0;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = 0;

        // Always preserve file partitions.
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .preserve_file_partitions = 1;
        // Set to a high value to ensure we always use the subset satisfaction optimization.
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .subset_satisfaction_partition_threshold = 999;
    }
}
