#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::{
        array::RecordBatch,
        datatypes::DataType,
        util::pretty::{self, pretty_format_batches},
    };
    use datafusion::{
        common::{ScalarValue, SplitPoint},
        datasource::{
            file_format::parquet::ParquetFormat,
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        },
        error::Result,
        logical_expr::{Partitioning, RangePartitioning},
        physical_plan::collect,
        prelude::{SessionContext, col},
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, assert_snapshot, display_plan_ascii,
        test_utils::{localhost::start_localhost_context, routing::UrlEmitterRouteTaskHandler},
    };
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_join_range_partitioned() -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            SELECT 
                f.timestamp,
                f.value,
                d.env,
                d.service,
                d.host
            FROM dim d
            INNER JOIN fact f ON d.d_dkey = f.f_dkey
            WHERE d.service = 'log'
            ORDER BY env, timestamp
        "#;

        // Use a four-worker context with range-partitioned tables.
        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(4, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_tables(&distributed_ctx, Path::new("testdata/join/parquet")).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // Matching ranges let the join run without shuffling its inputs.
        assert_snapshot!(&distributed_plan,
        @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=8
          │ SortExec: expr=[env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[timestamp@4, value@5, env@0, service@1, host@2]
          │     FilterExec: service@1 = log
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [], []]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │         t1: DataSourceExec: file_groups={4 groups: [[], [], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [], []]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │       t1: DataSourceExec: file_groups={4 groups: [[], [], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          └──────────────────────────────────────────────────
        ");

        // Ensure distributed results are correct.
        let pretty_results = pretty_format_batches(&distributed_results)?;
        assert_snapshot!(pretty_results,
        @"
        +---------------------+-------+------+---------+--------+
        | timestamp           | value | env  | service | host   |
        +---------------------+-------+------+---------+--------+
        | 2023-01-01T09:00:00 | 95.5  | dev  | log     | host-y |
        | 2023-01-01T09:00:10 | 102.3 | dev  | log     | host-y |
        | 2023-01-01T09:00:20 | 98.7  | dev  | log     | host-y |
        | 2023-01-01T09:12:20 | 105.1 | dev  | log     | host-y |
        | 2023-01-01T09:12:30 | 100.0 | dev  | log     | host-y |
        | 2023-01-01T09:12:40 | 150.0 | dev  | log     | host-y |
        | 2023-01-01T09:12:50 | 120.8 | dev  | log     | host-y |
        | 2023-01-01T09:00:00 | 75.2  | prod | log     | host-x |
        | 2023-01-01T09:00:10 | 82.4  | prod | log     | host-x |
        | 2023-01-01T09:00:20 | 78.9  | prod | log     | host-x |
        | 2023-01-01T09:00:30 | 85.6  | prod | log     | host-x |
        | 2023-01-01T09:12:30 | 80.0  | prod | log     | host-x |
        | 2023-01-01T09:12:40 | 120.0 | prod | log     | host-x |
        | 2023-01-01T09:12:50 | 92.3  | prod | log     | host-x |
        +---------------------+-------+------+---------+--------+
        ");

        Ok(())
    }

    #[tokio::test]
    async fn test_join_agg_range_partitioned() -> Result<(), Box<dyn std::error::Error>> {
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

        // Use a four-worker context with range-partitioned tables.
        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(4, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_tables(&distributed_ctx, Path::new("testdata/join/parquet")).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // Matching ranges keep both the join and key-preserving aggregation task-local.
        assert_snapshot!(&distributed_plan, @r#"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=8
          │ ProjectionExec: expr=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp)@1 as time_bin, env@2 as env, max(j.value)@3 as max_bin_value]
          │   AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }, timestamp@2) as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp), env@1 as env], aggr=[max(j.value)], ordering_mode=PartiallySorted([0, 1])
          │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@2)], projection=[f_dkey@4, env@0, timestamp@2, value@3]
          │       FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
          │         DistributedLeafExec:
          │           t0: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [], []]}, projection=[env, service, d_dkey], output_partitioning=Range([d_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │           t1: DataSourceExec: file_groups={4 groups: [[], [], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], output_partitioning=Range([d_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [], []]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          │         t1: DataSourceExec: file_groups={4 groups: [[], [], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
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
    async fn test_join_time_space_agg_range_partitioned() -> Result<(), Box<dyn std::error::Error>>
    {
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

        // Use a four-worker context with range-partitioned tables.
        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(4, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        register_tables(&distributed_ctx, Path::new("testdata/join/parquet")).await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;

        // The join and key-preserving aggregation stay task-local; grouping by env and
        // time requires a shuffle.
        assert_snapshot!(&distributed_plan, @r#"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [env@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=2
          │ ProjectionExec: expr=[env@0 as env, time_bin@1 as time_bin, avg(a.max_bin_value)@2 as avg_max_value]
          │   SortExec: expr=[env@0 ASC NULLS LAST, time_bin@1 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=FinalPartitioned, gby=[env@0 as env, time_bin@1 as time_bin], aggr=[avg(a.max_bin_value)]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=4
            │ RepartitionExec: partitioning=Hash([env@0, time_bin@1], 4), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[env@1 as env, time_bin@0 as time_bin], aggr=[avg(a.max_bin_value)]
            │     ProjectionExec: expr=[date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp)@1 as time_bin, env@2 as env, max(j.value)@3 as max_bin_value]
            │       AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }, timestamp@2) as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp), env@1 as env], aggr=[max(j.value)], ordering_mode=PartiallySorted([0, 1])
            │         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@2)], projection=[f_dkey@4, env@0, timestamp@2, value@3]
            │           FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
            │             DistributedLeafExec:
            │               t0: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [], []]}, projection=[env, service, d_dkey], output_partitioning=Range([d_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │               t1: DataSourceExec: file_groups={4 groups: [[], [], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], output_partitioning=Range([d_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │           DistributedLeafExec:
            │             t0: DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [], []]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │             t1: DataSourceExec: file_groups={4 groups: [[], [], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[timestamp, value, f_dkey], output_ordering=[f_dkey@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
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

    #[tokio::test]
    async fn test_join_range_partitioned_multiple_files() -> Result<()> {
        let data_dir = multiple_files_per_partition()?;
        let single_node_ctx = SessionContext::new();
        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(4, DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        for ctx in [&single_node_ctx, &distributed_ctx] {
            // Isolate partition assignment from dynamic-filter behavior.
            ctx.sql("SET datafusion.optimizer.enable_dynamic_filter_pushdown = false")
                .await?;
            register_tables(ctx, &data_dir).await?;
        }

        let query = r#"
            SELECT d.env, COUNT(*) AS n
            FROM dim d
            JOIN fact f ON d.d_dkey = f.f_dkey
            WHERE d.service = 'log'
            GROUP BY d.env
            ORDER BY d.env
        "#;
        let expected = single_node_ctx.sql(query).await?.collect().await?;
        let (plan, actual) = execute_query(&distributed_ctx, query).await?;
        fs::remove_dir_all(data_dir)?;

        assert!(plan.contains("HashJoinExec: mode=Partitioned"), "{plan}");
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            pretty_format_batches(&expected)?.to_string(),
            "{plan}",
        );
        Ok(())
    }

    fn set_configs(ctx: &mut SessionContext) {
        ctx.set_distributed_route_task_handler(UrlEmitterRouteTaskHandler);
        let state = ctx.state_ref();
        let mut state = state.write();
        let options = state.config_mut().options_mut();
        options.execution.target_partitions = 2;
        options.optimizer.hash_join_single_partition_threshold = 0;
        options.optimizer.hash_join_single_partition_threshold_rows = 0;
    }

    fn multiple_files_per_partition() -> Result<PathBuf> {
        let dir = std::env::temp_dir().join(format!("copartitioned_join_{}", Uuid::new_v4()));
        // Matching keys must join across all files, not just files with matching indices.
        for (table, column, files) in [("dim", "d_dkey", 2), ("fact", "f_dkey", 3)] {
            for key in ["A", "B", "C", "D"] {
                let partition = Path::new(table).join(format!("{column}={key}"));
                let target = dir.join(&partition);
                fs::create_dir_all(&target)?;
                for file in 0..files {
                    fs::copy(
                        Path::new("testdata/join/parquet")
                            .join(&partition)
                            .join("data0.parquet"),
                        target.join(format!("data{file}.parquet")),
                    )?;
                }
            }
        }
        Ok(dir)
    }

    async fn register_tables(ctx: &SessionContext, data_dir: &Path) -> Result<()> {
        for (name, partition_column) in [("dim", "d_dkey"), ("fact", "f_dkey")] {
            let output_partitioning = Partitioning::Range(RangePartitioning::try_new(
                vec![col(partition_column).sort(true, false)],
                ["B", "C", "D"]
                    .into_iter()
                    .map(|key| SplitPoint::new(vec![ScalarValue::Utf8(Some(key.to_string()))]))
                    .collect(),
            )?);
            let mut options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_table_partition_cols(vec![(partition_column.to_string(), DataType::Utf8)])
                .with_output_partitioning(Some(output_partitioning));
            if name == "fact" {
                options = options.with_file_sort_order(vec![vec![
                    col(partition_column).sort(true, false),
                    col("timestamp").sort(true, false),
                ]]);
            }
            let config = ListingTableConfig::new(ListingTableUrl::parse(
                data_dir.join(name).to_str().unwrap(),
            )?)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await?;
            ctx.register_table(name, Arc::new(ListingTable::try_new(config)?))?;
        }
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

        let distributed_results = collect(physical_plan, state.task_ctx()).await?;
        pretty::print_batches(&distributed_results)?;
        Ok((distributed_plan, distributed_results))
    }
}
