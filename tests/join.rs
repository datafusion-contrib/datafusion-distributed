#[cfg(test)]
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

    fn set_configs(ctx: &SessionContext) {
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
            // TODO: Figure out why file sort order does not display in plan.
            .file_sort_order(vec![vec![
                col("f_dkey").sort(true, true),
                col("timestamp").sort(true, true),
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
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        let distributed_plan = display_plan_ascii(physical_plan.as_ref(), false);
        println!("\n——————— DISTRIBUTED PLAN ———————\n\n{}", distributed_plan);

        let distributed_results = collect(physical_plan, state.task_ctx()).await?;
        pretty::print_batches(&distributed_results)?;
        Ok((distributed_plan, distributed_results))
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

        // —————————————————————————————————————————————————————————————
        // Execute the query using distributed datafusion, 2 workers,
        // and hive-style partitioned data.
        // —————————————————————————————————————————————————————————————

        let (distributed_ctx, _guard) = start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs(&distributed_ctx);
        register_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // —————————————————————————————————————————————————————————————
        // Ensure the distributed plan matches our target plan, registering
        // hive-style partitioning and avoiding data-shuffling repartitions.
        // —————————————————————————————————————————————————————————————

        assert_snapshot!(&distributed_plan,
        @"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] 
          │ SortExec: expr=[f_dkey@0 ASC NULLS LAST, timestamp@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[f_dkey@5 as f_dkey, timestamp@3 as timestamp, value@4 as value, env@0 as env, service@1 as service, host@2 as host]
          │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[env@0, service@1, host@2, timestamp@4, value@5, f_dkey@6]
          │       FilterExec: service@1 = log
          │         PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1] 
          │           DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, host, d_dkey], file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │       PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1] 
          │         DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data2.parquet, /testdata/join/parquet/fact/f_dkey=B/data0.parquet, /testdata/join/parquet/fact/f_dkey=B/data1.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet, /testdata/join/parquet/fact/f_dkey=C/data1.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
        ");

        // —————————————————————————————————————————————————————————————
        // Ensure distributed results are correct.
        // —————————————————————————————————————————————————————————————

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
        | B      | 2023-01-01T10:00:00 | 88.5  | prod | log     | host-x |
        | B      | 2023-01-01T10:00:10 | 91.2  | prod | log     | host-x |
        | B      | 2023-01-01T10:00:20 | 87.3  | prod | log     | host-x |
        | B      | 2023-01-01T10:00:30 | 94.1  | prod | log     | host-x |
        | B      | 2023-01-01T10:12:30 | 89.5  | prod | log     | host-x |
        | B      | 2023-01-01T10:12:40 | 95.8  | prod | log     | host-x |
        | B      | 2023-01-01T11:00:00 | 72.8  | prod | log     | host-x |
        | B      | 2023-01-01T11:00:10 | 79.4  | prod | log     | host-x |
        | B      | 2023-01-01T11:00:20 | 76.1  | prod | log     | host-x |
        | B      | 2023-01-01T11:00:30 | 83.7  | prod | log     | host-x |
        | B      | 2023-01-01T11:12:30 | 77.2  | prod | log     | host-x |
        +--------+---------------------+-------+------+---------+--------+
        ");

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

        // —————————————————————————————————————————————————————————————
        // Execute the query using distributed datafusion, 2 workers,
        // and hive-style partitioned data.
        // —————————————————————————————————————————————————————————————

        let (distributed_ctx, _guard) = start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs(&distributed_ctx);
        register_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // —————————————————————————————————————————————————————————————
        // Ensure the distributed plan matches our target plan, registering
        // hive-style partitioning and avoiding data-shuffling repartitions.
        // —————————————————————————————————————————————————————————————

        assert_snapshot!(&distributed_plan, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p1] t1:[p2..p3] 
          │ SortExec: expr=[f_dkey@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp)@1 as time_bin, env@2 as env, max(j.value)@3 as max_bin_value]
          │     AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }, timestamp@2) as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp), env@1 as env], aggr=[max(j.value)], ordering_mode=PartiallySorted([0, 1])
          │       ProjectionExec: expr=[f_dkey@3 as f_dkey, env@0 as env, timestamp@1 as timestamp, value@2 as value]
          │         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@2)], projection=[env@0, timestamp@2, value@3, f_dkey@4]
          │           FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
          │             PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1] 
          │               DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │           PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1] 
          │             DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data2.parquet, /testdata/join/parquet/fact/f_dkey=B/data0.parquet, /testdata/join/parquet/fact/f_dkey=B/data1.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet, /testdata/join/parquet/fact/f_dkey=C/data1.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
        "#);

        // —————————————————————————————————————————————————————————————
        // Ensure distributed results are correct.
        // —————————————————————————————————————————————————————————————

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
        | B      | 2023-01-01T10:00:00 | prod | 91.2          |
        | B      | 2023-01-01T10:00:30 | prod | 94.1          |
        | B      | 2023-01-01T10:12:30 | prod | 95.8          |
        | B      | 2023-01-01T11:00:00 | prod | 79.4          |
        | B      | 2023-01-01T11:00:30 | prod | 83.7          |
        | B      | 2023-01-01T11:12:30 | prod | 77.2          |
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

        // —————————————————————————————————————————————————————————————
        // Execute the query using distributed datafusion, 2 workers,
        // and hive-style partitioned data.
        // —————————————————————————————————————————————————————————————

        let (distributed_ctx, _guard) = start_localhost_context(2, DefaultSessionBuilder).await;
        set_configs(&distributed_ctx);
        register_tables(&distributed_ctx).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // —————————————————————————————————————————————————————————————
        // Ensure the distributed plan matches our target plan, registering
        // hive-style partitioning and avoiding data-shuffling repartitions.
        // —————————————————————————————————————————————————————————————

        assert_snapshot!(&distributed_plan, @r#"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [env@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST]
        │   SortExec: expr=[env@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST], preserve_partitioning=[true]
        │     ProjectionExec: expr=[env@0 as env, time_bin@1 as time_bin, avg(a.max_bin_value)@2 as avg_max_value]
        │       AggregateExec: mode=FinalPartitioned, gby=[env@0 as env, time_bin@1 as time_bin], aggr=[avg(a.max_bin_value)]
        │         [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0..p3] t1:[p0..p3] 
          │ CoalesceBatchesExec: target_batch_size=8192
          │   RepartitionExec: partitioning=Hash([env@0, time_bin@1], 4), input_partitions=2
          │     AggregateExec: mode=Partial, gby=[env@1 as env, time_bin@0 as time_bin], aggr=[avg(a.max_bin_value)]
          │       ProjectionExec: expr=[date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp)@1 as time_bin, env@2 as env, max(j.value)@3 as max_bin_value]
          │         AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }, timestamp@2) as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp), env@1 as env], aggr=[max(j.value)], ordering_mode=PartiallySorted([0, 1])
          │           ProjectionExec: expr=[f_dkey@3 as f_dkey, env@0 as env, timestamp@1 as timestamp, value@2 as value]
          │             HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@2)], projection=[env@0, timestamp@2, value@3, f_dkey@4]
          │               FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
          │                 PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1] 
          │                   DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │               PartitionIsolatorExec: t0:[p0,p1,__,__] t1:[__,__,p0,p1] 
          │                 DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data2.parquet, /testdata/join/parquet/fact/f_dkey=B/data0.parquet, /testdata/join/parquet/fact/f_dkey=B/data1.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet, /testdata/join/parquet/fact/f_dkey=C/data1.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], file_type=parquet, predicate=DynamicFilter [ empty ]
          └──────────────────────────────────────────────────
        "#);

        // —————————————————————————————————————————————————————————————
        // Ensure distributed results are correct.
        // —————————————————————————————————————————————————————————————

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
        | prod | 2023-01-01T10:00:00 | 91.2          |
        | prod | 2023-01-01T10:00:30 | 94.1          |
        | prod | 2023-01-01T10:12:30 | 95.8          |
        | prod | 2023-01-01T11:00:00 | 79.4          |
        | prod | 2023-01-01T11:00:30 | 83.7          |
        | prod | 2023-01-01T11:12:30 | 77.2          |
        +------+---------------------+---------------+
        ");

        Ok(())
    }
}
