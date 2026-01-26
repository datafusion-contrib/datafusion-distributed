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
        let (ctx_distributed, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;

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
        let (ctx_distributed, _guard, _) = start_localhost_context(6, DefaultSessionBuilder).await;

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
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
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

    /// Demonstrate a gather tree on a query that ends in a global ORDER BY, by creating many
    /// upstream tasks via a UNION and keeping enough workers available.
    #[tokio::test]
    async fn gather_tree_demo_aggregation_order_by() -> Result<(), Box<dyn Error>> {
        let (ctx_distributed, _guard, _) = start_localhost_context(16, DefaultSessionBuilder).await;

        // Reduce cardinality-driven task shrinking so we can clearly see the gather tree.
        ctx_distributed
            .sql("SET distributed.cardinality_task_count_factor=1.0;")
            .await?;
        ctx_distributed
            .sql("SET distributed.children_isolator_unions=true;")
            .await?;
        ctx_distributed
            .sql("SET distributed.coalesce_tree_min_input_tasks=8;")
            .await?;

        let query = r#"
        WITH u AS (
            SELECT "RainToday" FROM weather WHERE "MinTemp" > 10.0
            UNION ALL
            SELECT "RainToday" FROM weather WHERE "MaxTemp" < 30.0
            UNION ALL
            SELECT "RainToday" FROM weather WHERE "Temp9am" > 15.0
            UNION ALL
            SELECT "RainToday" FROM weather WHERE "Temp3pm" < 25.0
            UNION ALL
            SELECT "RainToday" FROM weather WHERE "Rainfall" > 5.0
            UNION ALL
            SELECT "RainToday" FROM weather WHERE "Evaporation" > 0.0
        )
        SELECT count(*), "RainToday" FROM u GROUP BY "RainToday" ORDER BY count(*)
        "#;

        register_parquet_tables(&ctx_distributed).await?;
        let df_distributed = ctx_distributed.sql(query).await?;
        let physical_distributed = df_distributed.create_physical_plan().await?;
        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(
            physical_distributed_str,
            @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 3] => NetworkCoalesceExec: output_partitions=48, input_tasks=4
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0..p11] t1:[p0..p11] t2:[p0..p11] t3:[p0..p11] 
          │ [Stage 2] => NetworkCoalesceExec: output_partitions=12, input_tasks=16
          └──────────────────────────────────────────────────
            ┌───── Stage 2 ── Tasks: t0:[p0..p2] t1:[p0..p2] t2:[p0..p2] t3:[p0..p2] t4:[p0..p2] t5:[p0..p2] t6:[p0..p2] t7:[p0..p2] t8:[p0..p2] t9:[p0..p2] t10:[p0..p2] t11:[p0..p2] t12:[p0..p2] t13:[p0..p2] t14:[p0..p2] t15:[p0..p2] 
            │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
            │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
            │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=16
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── Tasks: t0:[p0..p47] t1:[p0..p47] t2:[p0..p47] t3:[p0..p47] t4:[p0..p47] t5:[p0..p47] t6:[p0..p47] t7:[p0..p47] t8:[p0..p47] t9:[p0..p47] t10:[p0..p47] t11:[p0..p47] t12:[p0..p47] t13:[p0..p47] t14:[p0..p47] t15:[p0..p47] 
              │ RepartitionExec: partitioning=Hash([RainToday@0], 48), input_partitions=3
              │   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
              │     DistributedUnionExec: t0:[c0(0/2)] t1:[c0(1/2)] t2:[c1(0/2)] t3:[c1(1/2)] t4:[c2(0/3)] t5:[c2(1/3)] t6:[c2(2/3)] t7:[c3(0/3)] t8:[c3(1/3)] t9:[c3(2/3)] t10:[c4(0/3)] t11:[c4(1/3)] t12:[c4(2/3)] t13:[c5(0/3)] t14:[c5(1/3)] t15:[c5(2/3)]
              │       FilterExec: MinTemp@0 > 10, projection=[RainToday@1]
              │         PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
              │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=MinTemp@0 > 10, pruning_predicate=MinTemp_null_count@1 != row_count@2 AND MinTemp_max@0 > 10, required_guarantees=[]
              │       FilterExec: MaxTemp@0 < 30, projection=[RainToday@1]
              │         PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
              │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=MaxTemp@1 < 30, pruning_predicate=MaxTemp_null_count@1 != row_count@2 AND MaxTemp_min@0 < 30, required_guarantees=[]
              │       FilterExec: Temp9am@0 > 15, projection=[RainToday@1]
              │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
              │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp9am, RainToday], file_type=parquet, predicate=Temp9am@17 > 15, pruning_predicate=Temp9am_null_count@1 != row_count@2 AND Temp9am_max@0 > 15, required_guarantees=[]
              │       FilterExec: Temp3pm@0 < 25, projection=[RainToday@1]
              │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
              │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Temp3pm, RainToday], file_type=parquet, predicate=Temp3pm@18 < 25, pruning_predicate=Temp3pm_null_count@1 != row_count@2 AND Temp3pm_min@0 < 25, required_guarantees=[]
              │       FilterExec: Rainfall@0 > 5, projection=[RainToday@1]
              │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
              │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall, RainToday], file_type=parquet, predicate=Rainfall@2 > 5, pruning_predicate=Rainfall_null_count@1 != row_count@2 AND Rainfall_max@0 > 5, required_guarantees=[]
              │       FilterExec: Evaporation@0 > 0, projection=[RainToday@1]
              │         PartitionIsolatorExec: t0:[p0,__,__] t1:[__,p0,__] t2:[__,__,p0] 
              │           DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Evaporation, RainToday], file_type=parquet, predicate=Evaporation@3 > 0, pruning_predicate=Evaporation_null_count@1 != row_count@2 AND Evaporation_max@0 > 0, required_guarantees=[]
              └──────────────────────────────────────────────────
        "
        );

        // Sanity-check it executes.
        let _ = execute_stream(physical_distributed, ctx_distributed.task_ctx())?
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }

    /// Test that multiple first_value() aggregations work correctly in distributed queries.
    // TODO: Once https://github.com/apache/datafusion/pull/18303 is merged, this test will lose
    //       meaning, since the PR above will mask the underlying problem. Different queries or
    //       a new approach must be used in this case.
    #[tokio::test]
    async fn test_multiple_first_value_aggregations() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard, _) = start_localhost_context(3, DefaultSessionBuilder).await;

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
