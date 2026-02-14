#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::{
        array::RecordBatch,
        datatypes::DataType,
        util::pretty::{self, pretty_format_batches},
    };
    use datafusion::{
        error::Result,
        physical_plan::collect,
        prelude::{ParquetReadOptions, SessionContext, col},
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, assert_snapshot, display_plan_ascii,
        test_utils::localhost::start_localhost_context,
    };
    use std::sync::Arc;

    fn set_configs(ctx: &mut SessionContext) {
        // Preserve hive-style file partitions.
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .preserve_file_partitions = 1;
        // Read data from 4 hive-style partitions.
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 4;
        // Ensure that we use a partitioned hash join.
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
        // Enable dynamic filter pushdown
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .optimizer
            .enable_dynamic_filter_pushdown = true;
    }

    async fn register_tables(ctx: &SessionContext) -> Result<()> {
        // Register hive-style partitioning for the dim table.
        let dim_options = ParquetReadOptions::default()
            .table_partition_cols(vec![("d_dkey".to_string(), DataType::Utf8)]);
        ctx.register_parquet("dim", "testdata/join/parquet/dim", dim_options)
            .await?;

        // Register hive-style partitioning for the fact table.
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

    async fn execute_query(
        ctx: &SessionContext,
        query: &'static str,
    ) -> Result<(String, Vec<RecordBatch>)> {
        let df = ctx.sql(query).await?;
        let (state, logical_plan) = df.into_parts();

        // Create physical plan WITHOUT distributed optimizer
        use datafusion::execution::SessionStateBuilder;
        let mut state_builder = SessionStateBuilder::new_from_existing(state.clone());
        state_builder = state_builder.with_physical_optimizer_rules(vec![]);
        let non_distributed_state = state_builder.build();
        let non_distributed_plan = non_distributed_state.create_physical_plan(&logical_plan).await?;

        println!("\n——————— BEFORE DISTRIBUTED OPTIMIZER ———————\n");
        print_datasource_predicates(&non_distributed_plan, 0);
        println!("\n——————— ARC ADDRESSES BEFORE DISTRIBUTED OPTIMIZER ———————\n");
        print_dynamic_filter_arc_addresses(&non_distributed_plan, 0);

        // Create physical plan WITH distributed optimizer
        let physical_plan = state.create_physical_plan(&logical_plan).await?;

        println!("\n——————— AFTER DISTRIBUTED OPTIMIZER ———————\n");
        print_datasource_predicates(&physical_plan, 0);
        println!("\n——————— ARC ADDRESSES AFTER DISTRIBUTED OPTIMIZER ———————\n");
        print_dynamic_filter_arc_addresses(&physical_plan, 0);

        let distributed_plan = display_plan_ascii(physical_plan.as_ref(), false);
        println!("\n——————— DISTRIBUTED PLAN ———————\n\n{distributed_plan}");

        let distributed_results = collect(physical_plan.clone(), state.task_ctx()).await?;
        pretty::print_batches(&distributed_results)?;

        // Print plan with metrics after execution
        let distributed_plan_with_metrics = display_plan_ascii(physical_plan.as_ref(), true);
        println!("\n——————— PLAN WITH METRICS ———————\n\n{distributed_plan_with_metrics}");

        // Print detailed metrics from the physical plan
        use datafusion::physical_plan::displayable;
        let metrics_str = displayable(physical_plan.as_ref()).indent(true).to_string();
        println!("\n——————— DETAILED METRICS ———————\n\n{metrics_str}");

        Ok((distributed_plan, distributed_results))
    }

    fn print_datasource_predicates(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, depth: usize) {
        use datafusion::physical_plan::displayable;
        let indent = "  ".repeat(depth);
        let plan_name = plan.name();

        // Use displayable format which shows predicates
        if plan_name.starts_with("DataSourceExec") || plan_name.starts_with("ParquetExec") {
            let display_str = displayable(plan.as_ref()).indent(false).to_string();
            println!("{indent}{}", display_str);
        }

        for child in plan.children() {
            print_datasource_predicates(child, depth + 1);
        }
    }

    fn print_dynamic_filter_arc_addresses(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, depth: usize) {
        use datafusion::physical_plan::joins::HashJoinExec;
        use datafusion::datasource::source::DataSourceExec;

        let indent = "  ".repeat(depth);

        // Check if this is a HashJoinExec and print its dynamic filter address
        if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
            if let Some(filter) = hash_join.dynamic_filter_for_test() {
                let ptr_addr = Arc::as_ptr(filter) as *const () as u64;
                println!("{}HashJoinExec dynamic filter Arc address: 0x{:x}", indent, ptr_addr);
            }
        }

        // Check if this is a DataSourceExec and print its filter address
        if let Some(data_source) = plan.as_any().downcast_ref::<DataSourceExec>() {
            if let Some(filter) = data_source.filter_for_test() {
                let ptr = Arc::as_ptr(&filter) as *const () as u64;
                println!("{}DataSourceExec filter Arc address: 0x{:x}", indent, ptr);
            }
        }

        for child in plan.children() {
            print_dynamic_filter_arc_addresses(child, depth + 1);
        }
    }

    #[tokio::test]
    async fn test_join_hive() -> Result<(), Box<dyn std::error::Error>> {
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
            WHERE d.service = 'log'
            ORDER BY f_dkey, timestamp
        "#;

        // Execute the query using distributed datafusion, 2 workers,
        // and hive-style partitioned data.
        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // Ensure the distributed plan matches our target plan, registering
        // hive-style partitioning and avoiding data-shuffling repartitions.
        assert_snapshot!(&distributed_plan,
        @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] 
          │ ProjectionExec: expr=[f_dkey@5 as f_dkey, timestamp@3 as timestamp, value@4 as value, env@0 as env, service@1 as service, host@2 as host]
          │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[env@0, service@1, host@2, timestamp@4, value@5, f_dkey@6]
          │     FilterExec: service@1 = log
          │       PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
          │         DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, host, d_dkey], file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │     PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
          │       DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
        ");

        // Ensure distributed results are correct.
        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results,
        @"
        +--------+---------------------+-------+------+---------+--------+
        | f_dkey | timestamp           | value | env  | service | host   |
        +--------+---------------------+-------+------+---------+--------+
        | A      | 2023-01-01T09:00:00 | 95.5  | dev  | log     | host-y |
        | A      | 2023-01-01T09:00:10 | 102.3 | dev  | log     | host-y |
        | A      | 2023-01-01T09:00:20 | 98.7  | dev  | log     | host-y |
        | A      | 2023-01-01T09:12:20 | 105.1 | dev  | log     | host-y |
        | A      | 2023-01-01T09:12:30 | 100.0 | dev  | log     | host-y |
        | A      | 2023-01-01T09:12:40 | 150.0 | dev  | log     | host-y |
        | A      | 2023-01-01T09:12:50 | 120.8 | dev  | log     | host-y |
        | B      | 2023-01-01T09:00:00 | 75.2  | prod | log     | host-x |
        | B      | 2023-01-01T09:00:10 | 82.4  | prod | log     | host-x |
        | B      | 2023-01-01T09:00:20 | 78.9  | prod | log     | host-x |
        | B      | 2023-01-01T09:00:30 | 85.6  | prod | log     | host-x |
        | B      | 2023-01-01T09:12:30 | 80.0  | prod | log     | host-x |
        | B      | 2023-01-01T09:12:40 | 120.0 | prod | log     | host-x |
        | B      | 2023-01-01T09:12:50 | 92.3  | prod | log     | host-x |
        +--------+---------------------+-------+------+---------+--------+
        ");

        Ok(())
    }

    /// Test showing dynamic filter behavior in Partitioned hash join mode.
    ///
    /// This test demonstrates a known correctness bug with dynamic filtering in distributed
    /// Partitioned mode: https://github.com/apache/datafusion/pull/20175
    ///
    /// Expected behavior comparison:
    ///
    /// ============================================================
    /// DISABLED (no dynamic filter) - WORKS CORRECTLY:
    /// ============================================================
    ///
    /// ┌───── DistributedExec ── Tasks: t0:[p0]
    /// │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
    /// │   metrics=[output_rows=14]
    /// │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
    /// └──────────────────────────────────────────────────
    ///   ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3]
    ///   │ ProjectionExec: expr=[f_dkey@5, timestamp@3, value@4, env@0, service@1, host@2]
    ///   │   metrics=[output_rows=14]
    ///   │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)]
    ///   │     metrics=[output_rows=14, input_batches=4, input_rows=24, probe_hit_rate=58% (14/24)]
    ///   │                                               ^^^^^^^^^^^  ← Reads ALL 24 rows
    ///   │     FilterExec: service@1 = log
    ///   │       PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
    ///   │         DataSourceExec: file_groups={4 groups: [d_dkey=A, B, C, D]}
    ///   │           predicate=service@1 = log
    ///   │           metrics=[output_rows=2]
    ///   │     PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
    ///   │       DataSourceExec: file_groups={4 groups: [f_dkey=A, B, C, D]}
    ///   │         metrics=[output_rows=24, output_batches=4,
    ///   │                  files_ranges_pruned_statistics=4 total → 4 matched,  ← ALL 4 files
    ///   │                  bytes_scanned=767]
    ///   └──────────────────────────────────────────────────
    ///
    /// Results: 1 batches, 14 total rows ✓
    ///
    ///
    /// ============================================================
    /// ENABLED (with dynamic filter) - BROKEN! RETURNS 0 ROWS:
    /// ============================================================
    ///
    /// ┌───── DistributedExec ── Tasks: t0:[p0]
    /// │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
    /// │   metrics=[output_rows=0]  ← NO OUTPUT!
    /// │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
    /// └──────────────────────────────────────────────────
    ///   ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3]
    ///   │ ProjectionExec: expr=[f_dkey@5, timestamp@3, value@4, env@0, service@1, host@2]
    ///   │   metrics=[output_rows=0]  ← NO OUTPUT!
    ///   │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)]
    ///   │     metrics=[output_rows=0, input_batches=0, input_rows=0, probe_hit_rate=N/A (0/0)]
    ///   │                                              ^^^^^^^^^^^  ← NO INPUT!
    ///   │     FilterExec: service@1 = log
    ///   │       PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
    ///   │         DataSourceExec: file_groups={4 groups: [d_dkey=A, B, C, D]}
    ///   │           predicate=service@1 = log
    ///   │           metrics=[output_rows=2]  ← Build side is fine
    ///   │     PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
    ///   │       DataSourceExec: file_groups={4 groups: [f_dkey=A, B, C, D]}
    ///   │         predicate=DynamicFilter [ empty ]
    ///   │                   ^^^^^^^^^^^^^^^^^^^^^^^^^
    ///   │                   NOTE: Shows "empty" because the plan is displayed on the coordinator,
    ///   │                   which doesn't have the dynamic filter state. On the workers, the filter
    ///   │                   exists but incorrectly prunes ALL data.
    ///   │         metrics=[output_rows=0, output_batches=0,
    ///   │                  files_ranges_pruned_statistics=4 total → 0 matched,  ← PRUNED ALL!
    ///   │                  bytes_scanned=0]  ← NO DATA READ!
    ///   └──────────────────────────────────────────────────
    ///
    /// Results: 0 batches, 0 total rows ✗  (Should be 14 rows!)
    ///
    /// KNOWN ISSUE: Dynamic filtering in Partitioned mode incorrectly prunes all rows,
    /// returning 0 results instead of 14. This is a correctness bug tracked at:
    /// https://github.com/apache/datafusion/pull/20175
    #[tokio::test]
    async fn test_join_hive_dynamic_filter_comparison() -> Result<(), Box<dyn std::error::Error>> {
        async fn run_with_dynamic_filter(enable: bool, query: &str) -> Result<(), Box<dyn std::error::Error>> {
            let (mut ctx, _guard, _) =
                start_localhost_context(2, DefaultSessionBuilder).await;

            // Use same config as test_join_hive to get Partitioned mode
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
            // Force Partitioned mode by setting thresholds to 0
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

            // Set dynamic filter flag
            ctx.state_ref()
                .write()
                .config_mut()
                .options_mut()
                .optimizer
                .enable_join_dynamic_filter_pushdown = enable;

            register_tables(&ctx).await?;

            println!("\n{}", "=".repeat(60));
            println!("Dynamic Filter Pushdown: {}", if enable { "ENABLED" } else { "DISABLED" });
            println!("{}\n", "=".repeat(60));

            let df = ctx.sql(query).await?;
            let mut physical_plan = df.create_physical_plan().await?;

            // Execute using execute_stream to preserve metrics
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

            println!("Results: {} batches, {} total rows\n", results.len(), results.iter().map(|b| b.num_rows()).sum::<usize>());

            Ok(())
        }

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
            WHERE d.service = 'log'
            ORDER BY f_dkey, timestamp
        "#;

        run_with_dynamic_filter(false, query).await?;
        println!("\n\n");
        run_with_dynamic_filter(true, query).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_join_agg_hive() -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            SELECT  f_dkey, 
                    date_bin(INTERVAL '30 seconds', timestamp) AS time_bin,
                    env,
                    MAX(value) AS max_bin_value
            FROM
                (
                SELECT 
                    f.f_dkey,
                    d.env,
                    d.service,
                    d.host,
                    f.timestamp,
                    f.value
                FROM dim d
                INNER JOIN fact f ON d.d_dkey = f.f_dkey
                WHERE service = 'log'
                ) AS j
            GROUP BY f_dkey, time_bin, env
            ORDER BY f_dkey, time_bin
        "#;

        // Execute the query using distributed datafusion, 2 workers,
        // and hive-style partitioned data.
        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // Ensure the distributed plan matches our target plan, registering
        // hive-style partitioning and avoiding data-shuffling repartitions.
        assert_snapshot!(&distributed_plan, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] 
          │ ProjectionExec: expr=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp)@1 as time_bin, env@2 as env, max(j.value)@3 as max_bin_value]
          │   AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }, timestamp@2) as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp), env@1 as env], aggr=[max(j.value)], ordering_mode=PartiallySorted([0, 1])
          │     ProjectionExec: expr=[f_dkey@3 as f_dkey, env@0 as env, timestamp@1 as timestamp, value@2 as value]
          │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@2)], projection=[env@0, timestamp@2, value@3, f_dkey@4]
          │         FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
          │           PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
          │             DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │         PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
          │           DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
        "#);

        // Ensure distributed results are correct.
        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
        +--------+---------------------+------+---------------+
        | f_dkey | time_bin            | env  | max_bin_value |
        +--------+---------------------+------+---------------+
        | A      | 2023-01-01T09:00:00 | dev  | 102.3         |
        | A      | 2023-01-01T09:12:00 | dev  | 105.1         |
        | A      | 2023-01-01T09:12:30 | dev  | 150.0         |
        | B      | 2023-01-01T09:00:00 | prod | 82.4          |
        | B      | 2023-01-01T09:00:30 | prod | 85.6          |
        | B      | 2023-01-01T09:12:30 | prod | 120.0         |
        +--------+---------------------+------+---------------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_join_time_space_agg_hive() -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            SELECT env, time_bin, AVG(max_bin_value) AS avg_max_value
            FROM
            (
                SELECT  f_dkey, 
                        date_bin(INTERVAL '30 seconds', timestamp) AS time_bin,
                        env,
                        MAX(value) AS max_bin_value
                FROM
                    (
                    SELECT 
                        f.f_dkey,
                        d.env,
                        d.service,
                        d.host,
                        f.timestamp,
                        f.value
                    FROM dim d
                    INNER JOIN fact f ON d.d_dkey = f.f_dkey
                    WHERE service = 'log'
                    ) AS j
                GROUP BY f_dkey, time_bin, env
            ) AS a
            GROUP BY env, time_bin
            ORDER BY env, time_bin
        "#;

        // Execute the query using distributed datafusion, 2 workers,
        // and hive-style partitioned data.
        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // Ensure the distributed plan matches our target plan, registering
        // hive-style partitioning and avoiding data-shuffling repartitions.
        assert_snapshot!(&distributed_plan, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [env@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST]
        │   SortExec: expr=[env@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST], preserve_partitioning=[true]
        │     ProjectionExec: expr=[env@0 as env, time_bin@1 as time_bin, avg(a.max_bin_value)@2 as avg_max_value]
        │       AggregateExec: mode=FinalPartitioned, gby=[env@0 as env, time_bin@1 as time_bin], aggr=[avg(a.max_bin_value)]
        │         [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ RepartitionExec: partitioning=Hash([env@0, time_bin@1], 4), input_partitions=2
          │   AggregateExec: mode=Partial, gby=[env@1 as env, time_bin@0 as time_bin], aggr=[avg(a.max_bin_value)]
          │     ProjectionExec: expr=[date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp)@1 as time_bin, env@2 as env, max(j.value)@3 as max_bin_value]
          │       AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }, timestamp@2) as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp), env@1 as env], aggr=[max(j.value)], ordering_mode=PartiallySorted([0, 1])
          │         ProjectionExec: expr=[f_dkey@3 as f_dkey, env@0 as env, timestamp@1 as timestamp, value@2 as value]
          │           HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@2)], projection=[env@0, timestamp@2, value@3, f_dkey@4]
          │             FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
          │               PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
          │                 DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │             PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
          │               DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
        "#);

        // Ensure distributed results are correct.
        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results, @"
        +------+---------------------+---------------+
        | env  | time_bin            | avg_max_value |
        +------+---------------------+---------------+
        | dev  | 2023-01-01T09:00:00 | 102.3         |
        | dev  | 2023-01-01T09:12:00 | 105.1         |
        | dev  | 2023-01-01T09:12:30 | 150.0         |
        | prod | 2023-01-01T09:00:00 | 82.4          |
        | prod | 2023-01-01T09:00:30 | 85.6          |
        | prod | 2023-01-01T09:12:30 | 120.0         |
        +------+---------------------+---------------+
        ");

        Ok(())
    }
}
