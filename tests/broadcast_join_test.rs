#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::execute_stream;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::{DefaultSessionBuilder, display_plan_ascii};
    use futures::TryStreamExt;
    use parquet::arrow::ArrowWriter;
    use std::error::Error;
    use std::sync::Arc;
    use uuid::Uuid;

    /// Helper function to create a small dimension table (100 rows) across multiple files
    async fn create_small_table(ctx: &SessionContext) -> Result<(), Box<dyn Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("dim_value", DataType::Utf8, false),
        ]));

        // Create temp directory for partitioned data
        use std::fs;
        let temp_dir = std::env::temp_dir();
        let table_dir = temp_dir.join(format!("small_table_{}", Uuid::new_v4()));
        fs::create_dir(&table_dir)?;

        // Create 100 rows split across 1 file (small table should be in one partition)
        let mut id_values = Vec::new();
        let mut dim_values = Vec::new();
        for i in 0..100 {
            id_values.push(i);
            dim_values.push(format!("dim_{}", i % 10));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(id_values)),
                Arc::new(StringArray::from(dim_values)),
            ],
        )?;

        // Write to parquet file
        let file_path = table_dir.join("part-0.parquet");
        let file = std::fs::File::create(&file_path)?;
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
        writer.write(&batch)?;
        writer.close()?;

        // Register as parquet table with directory
        ctx.register_parquet(
            "small_table",
            table_dir.to_str().unwrap(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await?;

        Ok(())
    }

    /// Helper function to create a large fact table (10,000 rows) across multiple files
    async fn create_large_table(ctx: &SessionContext) -> Result<(), Box<dyn Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("fact_value", DataType::Int32, false),
        ]));

        // Create temp directory for partitioned data
        use std::fs;
        let temp_dir = std::env::temp_dir();
        let table_dir = temp_dir.join(format!("large_table_{}", Uuid::new_v4()));
        fs::create_dir(&table_dir)?;

        // Create 10,000 rows split across 3 files (to match the 3 workers)
        let rows_per_file = 3334;
        for file_num in 0..3 {
            let mut id_values = Vec::new();
            let mut fact_values = Vec::new();

            let start_row = file_num * rows_per_file;
            let end_row = if file_num == 2 {
                10000 // Last file gets remaining rows
            } else {
                start_row + rows_per_file
            };

            for i in start_row..end_row {
                let id = i % 100; // Cycle through 0-99 to match small table
                id_values.push(id);
                fact_values.push(i);
            }

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(id_values)),
                    Arc::new(Int32Array::from(fact_values)),
                ],
            )?;

            // Write to parquet file
            let file_path = table_dir.join(format!("part-{}.parquet", file_num));
            let file = std::fs::File::create(&file_path)?;
            let mut writer = ArrowWriter::try_new(file, schema.clone(), None)?;
            writer.write(&batch)?;
            writer.close()?;
        }

        // Register as parquet table with directory
        ctx.register_parquet(
            "large_table",
            table_dir.to_str().unwrap(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_broadcast_join_basic() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard) = start_localhost_context(3, DefaultSessionBuilder).await;

        // Setup tables
        create_small_table(&ctx_distributed).await?;
        create_large_table(&ctx_distributed).await?;

        // Test query: join large table with small table
        // Use aggregation to force distributed execution
        let query = r#"
            SELECT COUNT(*) as total
            FROM large_table AS l
            JOIN small_table AS s
            ON l.id = s.id
        "#;

        let df = ctx_distributed.sql(query).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = display_plan_ascii(physical.as_ref(), false);

        println!("Physical plan:\n{}", physical_str);

        // Verify that the plan contains NetworkBroadcastExec
        assert!(
            physical_str.contains("NetworkBroadcast"),
            "Expected NetworkBroadcastExec in plan, got:\n{}",
            physical_str
        );

        let results = execute_stream(physical, ctx_distributed.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        assert!(!results.is_empty(), "Expected non-empty results");

        // Verify result
        let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(
            total_rows, 1,
            "Expected 1 row for COUNT(*), got {}",
            total_rows
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_broadcast_join_uses_multiple_tasks() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard) = start_localhost_context(4, DefaultSessionBuilder).await;

        // Setup tables
        create_small_table(&ctx_distributed).await?;
        create_large_table(&ctx_distributed).await?;

        let query = r#"
            SELECT COUNT(*) as total
            FROM large_table AS l
            JOIN small_table AS s
            ON l.id = s.id
        "#;

        let df = ctx_distributed.sql(query).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = display_plan_ascii(physical.as_ref(), false);

        // Verify that the plan contains NetworkBroadcastExec
        assert!(
            physical_str.contains("NetworkBroadcast"),
            "Expected NetworkBroadcastExec in plan"
        );

        // Check that there are multiple tasks being used (not forced to 1)
        // Stage 2 (where the join happens) should have multiple tasks like "t0:[p0] t1:[p1] t2:[p2]"
        assert!(
            physical_str.contains("Stage 2") && physical_str.contains("t1:"),
            "Expected multiple tasks in Stage 2 for distributed execution, plan:\n{}",
            physical_str
        );

        // Execute query
        let results = execute_stream(physical, ctx_distributed.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(
            results.len(),
            1,
            "Expected single result batch for COUNT(*)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_broadcast_join_correctness() -> Result<(), Box<dyn Error>> {
        // Setup distributed context with 3 workers
        let (ctx_distributed, _guard) = start_localhost_context(3, DefaultSessionBuilder).await;

        // Setup single-node context
        let ctx_single = SessionContext::default();
        *ctx_single.state_ref().write().config_mut() = ctx_distributed.copied_config();

        // Create tables in both contexts
        create_small_table(&ctx_distributed).await?;
        create_large_table(&ctx_distributed).await?;
        create_small_table(&ctx_single).await?;
        create_large_table(&ctx_single).await?;

        let query = r#"
            SELECT l.id, COUNT(*) as cnt, SUM(l.fact_value) as total
            FROM large_table AS l
            JOIN small_table AS s
            ON l.id = s.id
            GROUP BY l.id
            ORDER BY l.id
        "#;

        // Execute on single-node
        let df_single = ctx_single.sql(query).await?;
        let physical_single = df_single.create_physical_plan().await?;
        let results_single = execute_stream(physical_single, ctx_single.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let single_output = pretty_format_batches(&results_single)?;

        // Execute on distributed
        let df_distributed = ctx_distributed.sql(query).await?;
        let physical_distributed = df_distributed.create_physical_plan().await?;
        let physical_str = display_plan_ascii(physical_distributed.as_ref(), false);

        // Verify NetworkBroadcastExec is in the plan
        assert!(
            physical_str.contains("NetworkBroadcast"),
            "Expected NetworkBroadcastExec in distributed plan"
        );

        let results_distributed = execute_stream(physical_distributed, ctx_distributed.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;
        let distributed_output = pretty_format_batches(&results_distributed)?;

        // Compare results - they should be identical
        assert_eq!(
            single_output.to_string(),
            distributed_output.to_string(),
            "Distributed and single-node results should match.\nSingle:\n{}\nDistributed:\n{}",
            single_output,
            distributed_output
        );

        // Verify we got results for all 100 IDs
        let total_rows: usize = results_distributed
            .iter()
            .map(|batch| batch.num_rows())
            .sum();
        assert_eq!(
            total_rows, 100,
            "Expected 100 rows (one per ID), got {}",
            total_rows
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_broadcast_join_with_filter() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard) = start_localhost_context(3, DefaultSessionBuilder).await;

        create_small_table(&ctx_distributed).await?;
        create_large_table(&ctx_distributed).await?;

        // Test with WHERE clause to ensure filtering works correctly with broadcast
        let query = r#"
            SELECT l.id, COUNT(*) as cnt
            FROM large_table AS l
            JOIN small_table AS s
            ON l.id = s.id
            WHERE l.fact_value < 5000
            GROUP BY l.id
            ORDER BY l.id
        "#;

        let df = ctx_distributed.sql(query).await?;
        let physical = df.create_physical_plan().await?;

        let results = execute_stream(physical, ctx_distributed.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        assert!(
            !results.is_empty(),
            "Expected non-empty results with filter"
        );

        // Verify that we only get results for filtered data
        let total_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
        assert!(
            total_rows > 0 && total_rows <= 100,
            "Expected reasonable number of rows after filtering, got {}",
            total_rows
        );

        Ok(())
    }
}
