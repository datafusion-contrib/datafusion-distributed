#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::{
        error::Result,
        physical_plan::collect,
        prelude::{ParquetReadOptions, SessionContext, col},
    };
    use datafusion_distributed::{
        DefaultSessionBuilder,
        test_utils::localhost::start_localhost_context,
    };

    async fn register_tables(ctx: &SessionContext) -> Result<()> {
        let dim_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
        ctx.register_parquet("dim", "testdata/join/parquet/dim", dim_options)
            .await?;

        let fact_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("f_dkey".to_string(), DataType::Utf8)])
            .file_sort_order(vec![vec![
                col("f_dkey").sort(true, false),
                col("timestamp").sort(true, false),
            ]]);
        ctx.register_parquet("fact", "testdata/join/parquet/fact", fact_options)
            .await?;
        Ok(())
    }

    async fn run_query_with_config(enable_dynamic_filter: bool) -> Result<()> {
        let (mut ctx, _guard, _workers) =
            start_localhost_context(2, DefaultSessionBuilder).await;

        // Configure context
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .preserve_file_partitions = 1;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 4;
        // Force CollectLeft mode by setting high thresholds
        // This makes the optimizer think the build side is always small enough to collect
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = usize::MAX;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = usize::MAX;

        // IMPORTANT: Set dynamic filter pushdown
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .enable_dynamic_filter_pushdown = enable_dynamic_filter;

        register_tables(&ctx).await?;

        let query = "
            SELECT * FROM dim d
            JOIN fact j ON d.d_dkey = j.f_dkey
            WHERE d.service = 'log'
            ORDER BY j.f_dkey, j.timestamp
            LIMIT 10
        ";

        println!("\n{}", "=".repeat(60));
        println!("Dynamic Filter Pushdown: {}", if enable_dynamic_filter { "ENABLED" } else { "DISABLED" });
        println!("{}\n", "=".repeat(60));

        let df = ctx.sql(query).await?;
        let mut physical_plan = df.create_physical_plan().await?;

        // Execute using execute_stream to preserve metrics in the plan
        use datafusion::physical_plan::execute_stream;
        use futures::TryStreamExt;
        let results = execute_stream(physical_plan.clone(), ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        // Rewrite plan with metrics collected from workers
        use datafusion_distributed::{display_plan_ascii, rewrite_distributed_plan_with_metrics, DistributedMetricsFormat};
        physical_plan = rewrite_distributed_plan_with_metrics(
            physical_plan.clone(),
            DistributedMetricsFormat::Aggregated,
        )?;

        // Display plan with metrics
        let plan_with_metrics = display_plan_ascii(physical_plan.as_ref(), true);
        println!("\nPlan with metrics:\n{}\n", plan_with_metrics);

        // How to verify dynamic filtering is working:
        // 1. Look for "predicate=DynamicFilter [ ... ]" in DataSourceExec
        //    - If it shows specific values (e.g., "f_dkey@2 >= A AND f_dkey@2 <= B"), the filter was updated
        //    - If it shows "DynamicFilter [ empty ]", the filter was NOT updated (Arc sharing broken)
        // 2. Check output_rows metrics:
        //    - With dynamic filtering: DataSourceExec output_rows should be reduced (e.g., 14 rows)
        //    - Without dynamic filtering: DataSourceExec output_rows would be higher (e.g., 24 rows)
        // 3. Check files_ranges_pruned_statistics:
        //    - With dynamic filtering: Should show "4 total → 2 matched" (only reading needed files)
        //    - Without dynamic filtering: Would show "4 total → 4 matched" (reading all files)
        println!("Results: {} batches, {} total rows\n", results.len(), results.iter().map(|b| b.num_rows()).sum::<usize>());

        Ok(())
    }

    #[tokio::test]
    async fn compare_dynamic_filter_metrics() -> Result<()> {
        println!("\n\n");
        run_query_with_config(false).await?;
        println!("\n\n");
        run_query_with_config(true).await?;
        Ok(())
    }
}
