#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::error::Result;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionContext, col};
    use datafusion_distributed::assert_snapshot;
    use std::error::Error;
    use std::sync::Arc;

    /// Sets up external Parquet tables for parallel execution optimization tests
    ///
    /// This creates two tables:
    /// - `context`: Contains context information with columns (bhandle, env, service, host)
    ///              WITH ORDER (env, service, host)
    /// - `points`: Contains time series data with columns (bhandle, timestamp, value)  
    ///             WITH ORDER (bhandle, timestamp) and partitioned by timestamp
    async fn setup_tables(ctx: &SessionContext) -> Result<(), Box<dyn Error>> {
        use datafusion::datasource::file_format::parquet::ParquetFormat;

        // Register context table from context_1.parquet
        let parquet_format_context = Arc::new(ParquetFormat::default());
        let context_listing_options = ListingOptions::new(parquet_format_context)
            .with_file_extension(".parquet")
            .with_target_partitions(1) // Single partition
            .with_collect_stat(true) // Enable statistics collection
            .with_file_sort_order(vec![vec![
                col("env").sort(true, false),     // ASC, nulls last
                col("service").sort(true, false), // ASC, nulls last
                col("host").sort(true, false),    // ASC, nulls last
            ]]);

        let context_table_path = ListingTableUrl::parse("testdata/test_opt/context1/")?;
        let state = ctx.state();
        let context_schema = context_listing_options
            .infer_schema(&state, &context_table_path)
            .await?;

        let context_config = ListingTableConfig::new(context_table_path)
            .with_listing_options(context_listing_options)
            .with_schema(context_schema);

        let context_table = Arc::new(ListingTable::try_new(context_config)?);
        ctx.register_table("context", context_table)?;

        // Register points table with one Parquet file per partition
        let parquet_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(parquet_format)
            .with_file_extension(".parquet")
            .with_target_partitions(2) // Two partitions (one per file)
            .with_collect_stat(true) // Enable statistics collection
            .with_file_sort_order(vec![vec![
                col("bhandle").sort(true, false),   // ASC, nulls last
                col("timestamp").sort(true, false), // ASC, nulls last
            ]]);

        // Create the table path pointing to the points1 directory (contains parquet files)
        let table_path = ListingTableUrl::parse("testdata/test_opt/points1/")?;

        // Infer schema from the parquet files
        let schema = listing_options.infer_schema(&state, &table_path).await?;

        // Create ListingTable configuration
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(listing_options)
            .with_schema(schema);

        // Create and register the ListingTable - this will treat each Parquet file as a partition
        let points_table = Arc::new(ListingTable::try_new(config)?);
        ctx.register_table("points", points_table)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_context_table() -> Result<(), Box<dyn Error>> {
        let ctx = SessionContext::new();

        // Setup tables
        setup_tables(&ctx)
            .await
            .expect("Failed to setup CSV tables");

        // Verify query results
        let context_query = "SELECT * FROM context ORDER BY env, service, host";
        let context_results = ctx.sql(context_query).await?.collect().await?;
        let formatted_results = pretty_format_batches(&context_results)?;
        assert_snapshot!(formatted_results, @r"
        +---------+------+---------+------+
        | bhandle | env  | service | host |
        +---------+------+---------+------+
        | A       | dev  | log     | ma   |
        | B       | prod | log     | ma   |
        | C       | prod | log     | vim  |
        | D       | prod | trace   | vim  |
        +---------+------+---------+------+
        ");

        // Display execution plan
        let context_df_explain = ctx.sql(context_query).await?;
        let physical_plan = context_df_explain.create_physical_plan().await?;
        let plan_display = displayable(physical_plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan_display, @"DataSourceExec: file_groups={1 group: [[/testdata/test_opt/context1/context_1.parquet]]}, projection=[bhandle, env, service, host], output_ordering=[env@1 ASC NULLS LAST, service@2 ASC NULLS LAST, host@3 ASC NULLS LAST], file_type=parquet");

        Ok(())
    }

    #[tokio::test]
    async fn test_points_table() -> Result<(), Box<dyn Error>> {
        let ctx = SessionContext::new();

        // Setup tables
        setup_tables(&ctx)
            .await
            .expect("Failed to setup CSV tables");

        // Verify query results
        let points_query = "SELECT * FROM points ORDER BY bhandle, timestamp";
        let points_results = ctx.sql(points_query).await?.collect().await?;
        let formatted_results = pretty_format_batches(&points_results)?;
        assert_snapshot!(formatted_results, @r"
        +---------+---------------------+-------+
        | bhandle | timestamp           | value |
        +---------+---------------------+-------+
        | A       | 2023-01-01T09:00:00 | 95.5  |
        | A       | 2023-01-01T09:00:10 | 102.3 |
        | A       | 2023-01-01T09:00:20 | 98.7  |
        | A       | 2023-01-01T09:12:20 | 105.1 |
        | A       | 2023-01-01T09:12:30 | 100.0 |
        | A       | 2023-01-01T09:12:40 | 150.0 |
        | A       | 2023-01-01T09:12:50 | 120.8 |
        | A       | 2023-01-01T10:00:00 | 18.5  |
        | A       | 2023-01-01T10:00:10 | 35.3  |
        | A       | 2023-01-01T10:00:20 | 55.7  |
        | A       | 2023-01-01T10:12:20 | 100.1 |
        | A       | 2023-01-01T10:12:30 | 44.0  |
        | A       | 2023-01-01T10:12:40 | 350.0 |
        | A       | 2023-01-01T10:12:50 | 320.8 |
        | B       | 2023-01-01T09:00:00 | 75.2  |
        | B       | 2023-01-01T09:00:10 | 82.4  |
        | B       | 2023-01-01T09:00:20 | 78.9  |
        | B       | 2023-01-01T09:00:30 | 85.6  |
        | B       | 2023-01-01T09:12:30 | 80.0  |
        | B       | 2023-01-01T09:12:40 | 120.0 |
        | B       | 2023-01-01T09:12:50 | 92.3  |
        | B       | 2023-01-01T10:00:00 | 175.2 |
        | B       | 2023-01-01T10:00:10 | 182.4 |
        | B       | 2023-01-01T10:00:20 | 278.9 |
        | B       | 2023-01-01T10:00:30 | 185.6 |
        | B       | 2023-01-01T10:12:30 | 810.0 |
        | B       | 2023-01-01T10:12:40 | 720.0 |
        | B       | 2023-01-01T10:12:50 | 222.3 |
        | C       | 2023-01-01T09:00:00 | 300.5 |
        | C       | 2023-01-01T09:00:10 | 285.7 |
        | C       | 2023-01-01T09:00:20 | 310.2 |
        | C       | 2023-01-01T09:00:30 | 295.8 |
        | C       | 2023-01-01T09:00:40 | 300.0 |
        | C       | 2023-01-01T09:12:40 | 250.0 |
        | C       | 2023-01-01T09:12:50 | 275.4 |
        | C       | 2023-01-01T10:00:00 | 310.5 |
        | C       | 2023-01-01T10:00:10 | 225.7 |
        | C       | 2023-01-01T10:00:20 | 380.2 |
        | C       | 2023-01-01T10:00:30 | 205.8 |
        | C       | 2023-01-01T10:00:40 | 350.0 |
        | C       | 2023-01-01T10:12:40 | 200.0 |
        | C       | 2023-01-01T10:12:50 | 205.4 |
        | D       | 2023-01-01T10:00:00 | 24.8  |
        | D       | 2023-01-01T10:00:10 | 72.1  |
        | D       | 2023-01-01T10:00:20 | 42.5  |
        +---------+---------------------+-------+
        ");

        // Display execution plan
        let points_df_explain = ctx.sql(points_query).await?;
        let physical_plan = points_df_explain.create_physical_plan().await?;
        let plan_display = displayable(physical_plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan_display, @r"
        SortPreservingMergeExec: [bhandle@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
          DataSourceExec: file_groups={2 groups: [[/testdata/test_opt/points1/points_1_1.parquet], [/testdata/test_opt/points1/points_1_2.parquet]]}, projection=[bhandle, timestamp, value], output_ordering=[bhandle@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST], file_type=parquet
        ");

        Ok(())
    }

    // This test ensures probe-side files remain in separate partitions to preserve data locality and sort order during the join.
    // It intentionally uses two probe-side files and one build-side file.
    #[tokio::test]
    async fn test_join_aggregation() -> Result<(), Box<dyn Error>> {
        // Create SessionContext with optimized settings for partitioned data
        use datafusion::prelude::SessionConfig;
        let config = SessionConfig::new()
            .with_target_partitions(1) // Keep context as single partition
            .with_repartition_joins(false) // Avoid repartitioning since data is already partitioned correctly
            .with_repartition_aggregations(false) // Avoid repartitioning for aggregations
            .with_repartition_file_scans(false) // Prevent repartitioning of file scans
            .with_collect_statistics(true) // Enable statistics collection for better optimization
            .with_prefer_existing_sort(true); // Prefer existing sort orders to avoid re-sorting

        let ctx = SessionContext::new_with_config(config);

        // Setup tables
        setup_tables(&ctx)
            .await
            .expect("Failed to setup CSV tables");

        // Verify query results
        let join_query = "SELECT  p.bhandle bhdl,
                    date_bin(INTERVAL '30 seconds', p.timestamp) time_bin,
                    c.service,
                    max(p.value) max_bin_val
            FROM  context c
            INNER JOIN points p ON c.bhandle = p.bhandle
            WHERE c.env = 'prod'
            GROUP BY bhdl, time_bin, service
            ORDER BY bhdl, time_bin, service";
        let join_results = ctx.sql(join_query).await?.collect().await?;
        let formatted_results = pretty_format_batches(&join_results)?;
        assert_snapshot!(formatted_results, @r"
        +------+---------------------+---------+-------------+
        | bhdl | time_bin            | service | max_bin_val |
        +------+---------------------+---------+-------------+
        | B    | 2023-01-01T09:00:00 | log     | 82.4        |
        | B    | 2023-01-01T09:00:30 | log     | 85.6        |
        | B    | 2023-01-01T09:12:30 | log     | 120.0       |
        | B    | 2023-01-01T10:00:00 | log     | 278.9       |
        | B    | 2023-01-01T10:00:30 | log     | 185.6       |
        | B    | 2023-01-01T10:12:30 | log     | 810.0       |
        | C    | 2023-01-01T09:00:00 | log     | 310.2       |
        | C    | 2023-01-01T09:00:30 | log     | 300.0       |
        | C    | 2023-01-01T09:12:30 | log     | 275.4       |
        | C    | 2023-01-01T10:00:00 | log     | 380.2       |
        | C    | 2023-01-01T10:00:30 | log     | 350.0       |
        | C    | 2023-01-01T10:12:30 | log     | 205.4       |
        | D    | 2023-01-01T10:00:00 | trace   | 72.1        |
        +------+---------------------+---------+-------------+
        ");

        // Display execution plan
        let join_df_explain = ctx.sql(join_query).await?;
        let physical_plan = join_df_explain.create_physical_plan().await?;
        let plan_display = displayable(physical_plan.as_ref()).indent(true).to_string();
        assert_snapshot!(plan_display, @r#"
        SortExec: expr=[bhdl@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST, service@2 ASC NULLS LAST], preserve_partitioning=[false]
          ProjectionExec: expr=[bhandle@0 as bhdl, date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),p.timestamp)@1 as time_bin, service@2 as service, max(p.value)@3 as max_bin_val]
            AggregateExec: mode=Final, gby=[bhandle@0 as bhandle, date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),p.timestamp)@1 as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),p.timestamp), service@2 as service], aggr=[max(p.value)], ordering_mode=PartiallySorted([0, 1])
              SortPreservingMergeExec: [bhandle@0 ASC NULLS LAST, date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),p.timestamp)@1 ASC NULLS LAST]
                AggregateExec: mode=Partial, gby=[bhandle@1 as bhandle, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }, timestamp@2) as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),p.timestamp), service@0 as service], aggr=[max(p.value)], ordering_mode=PartiallySorted([0, 1])
                  CoalesceBatchesExec: target_batch_size=8192
                    HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(bhandle@0, bhandle@0)], projection=[service@1, bhandle@2, timestamp@3, value@4]
                      CoalesceBatchesExec: target_batch_size=8192
                        FilterExec: env@1 = prod, projection=[bhandle@0, service@2]
                          DataSourceExec: file_groups={1 group: [[/testdata/test_opt/context1/context_1.parquet]]}, projection=[bhandle, env, service], output_ordering=[env@1 ASC NULLS LAST, service@2 ASC NULLS LAST], file_type=parquet, predicate=env@1 = prod, pruning_predicate=env_null_count@2 != row_count@3 AND env_min@0 <= prod AND prod <= env_max@1, required_guarantees=[env in (prod)]
                      DataSourceExec: file_groups={2 groups: [[/testdata/test_opt/points1/points_1_1.parquet], [/testdata/test_opt/points1/points_1_2.parquet]]}, projection=[bhandle, timestamp, value], output_ordering=[bhandle@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST], file_type=parquet, predicate=DynamicFilterPhysicalExpr [ true ]
        "#);

        Ok(())
    }

    // TODOs:
    // 1. Test with multiple build-side files grouped into a single partition (one hash table), since context files share similar content.
    //   Keep probe-side files in separate partitions to maintain data locality and sort order during the join.
    // 2. Test aggregation optimization using statistics and additional config or physical rules to enable single-step execution.
    // 3. Test with one more time aggregation in the query
}
