#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::test_utils::session_context::register_temp_parquet_table;
    use datafusion_distributed::{DefaultSessionBuilder, assert_snapshot, display_plan_ascii};
    use futures::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn distributed_aggregation() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard) = start_localhost_context(3, DefaultSessionBuilder).await;

        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;

        let ctx = SessionContext::default();
        *ctx.state_ref().write().config_mut() = ctx_distributed.copied_config();
        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        register_parquet_tables(&ctx_distributed).await?;
        let df_distributed = ctx_distributed.sql(query).await?;
        let physical_distributed = df_distributed.create_physical_plan().await?;
        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(physical_str,
            @r"
        ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
          SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
            SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
              ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
                AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  CoalesceBatchesExec: target_batch_size=8192
                    RepartitionExec: partitioning=Hash([RainToday@0], 3), input_partitions=3
                      AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                        DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ",
        );

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=1
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
            │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ",
        );

        let batches = pretty_format_batches(
            &execute_stream(physical, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        +----------+-----------+
        | count(*) | RainToday |
        +----------+-----------+
        | 66       | Yes       |
        | 300      | No        |
        +----------+-----------+
        ");

        let batches_distributed = pretty_format_batches(
            &execute_stream(physical_distributed, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;
        assert_snapshot!(batches_distributed, @r"
        +----------+-----------+
        | count(*) | RainToday |
        +----------+-----------+
        | 66       | Yes       |
        | 300      | No        |
        +----------+-----------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn distributed_aggregation_head_node_partitioned() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard) = start_localhost_context(6, DefaultSessionBuilder).await;

        let query = r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday""#;

        let ctx = SessionContext::default();
        *ctx.state_ref().write().config_mut() = ctx_distributed.copied_config();
        register_parquet_tables(&ctx).await?;
        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;
        let physical_str = displayable(physical.as_ref()).indent(true).to_string();

        register_parquet_tables(&ctx_distributed).await?;
        let df_distributed = ctx_distributed.sql(query).await?;
        let physical_distributed = df_distributed.create_physical_plan().await?;
        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(physical_str,
            @r"
        ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            CoalesceBatchesExec: target_batch_size=8192
              RepartitionExec: partitioning=Hash([RainToday@0], 3), input_partitions=3
                AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
        ",
        );

        assert_snapshot!(physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] 
          │ ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0..p5] t1:[p0..p5] t2:[p0..p5] 
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=1
            │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │     PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
            │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ",
        );

        Ok(())
    }

    /// Test that multiple first_value() aggregations work correctly in distributed queries.
    // TODO: Once https://github.com/apache/datafusion/pull/18303 is merged, this test will lose
    //       meaning, since the PR above will mask the underlying problem. Different queries or
    //       a new approach must be used in this case.
    #[tokio::test]
    async fn test_multiple_first_value_aggregations() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context(3, DefaultSessionBuilder).await;

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_id", DataType::Int32, false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Create 2 batches that will be stored as separate parquet files
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["trace1", "trace2"])),
                Arc::new(Int32Array::from(vec![100, 200])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["trace3", "trace4"])),
                Arc::new(Int32Array::from(vec![300, 400])),
            ],
        )?;

        let file1 =
            register_temp_parquet_table("records_part1", schema.clone(), vec![batch1], &ctx)
                .await?;
        let file2 =
            register_temp_parquet_table("records_part2", schema.clone(), vec![batch2], &ctx)
                .await?;

        // Create a partitioned table by registering multiple files
        let temp_dir = std::env::temp_dir();
        let table_dir = temp_dir.join(format!("partitioned_table_{}", Uuid::new_v4()));
        std::fs::create_dir(&table_dir)?;
        std::fs::copy(&file1, table_dir.join("part1.parquet"))?;
        std::fs::copy(&file2, table_dir.join("part2.parquet"))?;

        // Register the directory as a partitioned table
        ctx.register_parquet(
            "records_partitioned",
            table_dir.to_str().unwrap(),
            datafusion::prelude::ParquetReadOptions::default(),
        )
        .await?;

        let query = r#"SELECT group_id, first_value(trace_id) AS fv1, first_value(value) AS fv2
                       FROM records_partitioned 
                       GROUP BY group_id 
                       ORDER BY group_id"#;

        let df = ctx.sql(query).await?;
        let physical = df.create_physical_plan().await?;

        // Execute distributed query
        let batches_distributed = execute_stream(physical, ctx.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        let actual_result = pretty_format_batches(&batches_distributed)?;
        let expected_result = "\
+----------+--------+-----+
| group_id | fv1    | fv2 |
+----------+--------+-----+
| 1        | trace1 | 100 |
| 2        | trace2 | 200 |
| 3        | trace3 | 300 |
| 4        | trace4 | 400 |
+----------+--------+-----+";

        // Print them out, the error message from `assert_eq` is otherwise hard to read.
        println!("{expected_result}");
        println!("{actual_result}");

        // Compare against result. The regression this is testing for would have NULL values in
        // the second and third column.
        assert_eq!(actual_result.to_string(), expected_result,);

        Ok(())
    }
}
