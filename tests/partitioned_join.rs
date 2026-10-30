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
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(&["A", "B", "C", "D"], Some(2); "even")]
    #[test_case(&["A", "B", "C", "D"], Some(3); "uneven")]
    #[test_case(&["A", "B", "C", "D"], Some(5); "empty_task")]
    #[test_case(&["A", "B", "C", "D", "E"], None; "default_estimator_padding")]
    #[tokio::test]
    async fn test_join_range_partitioned(
        partition_keys: &[&str],
        task_count: Option<usize>,
    ) -> Result<()> {
        let query = r#"
            SELECT
                f.timestamp,
                f.value,
                d.env,
                d.service,
                d.host
            FROM dim d
            INNER JOIN fact f ON d.d_dkey = f.f_dkey
            ORDER BY env, timestamp, service, host, value
        "#;

        let single_node_ctx = SessionContext::new();
        let (mut distributed_ctx, _guard, _) =
            start_localhost_context(task_count.unwrap_or(4), DefaultSessionBuilder).await;
        set_configs(&mut distributed_ctx);
        if let Some(task_count) = task_count {
            distributed_ctx.set_distributed_desired_task_count_handler(task_count);
        }
        for ctx in [&single_node_ctx, &distributed_ctx] {
            // Isolate partition assignment from dynamic-filter behavior.
            ctx.sql("SET datafusion.optimizer.enable_dynamic_filter_pushdown = false")
                .await?;
            register_tables(ctx, partition_keys).await?;
        }

        let expected = single_node_ctx.sql(query).await?.collect().await?;
        let (distributed_plan, distributed_results) =
            execute_query(&distributed_ctx, query).await?;
        assert_eq!(
            pretty_format_batches(&distributed_results)?.to_string(),
            pretty_format_batches(&expected)?.to_string(),
        );

        // Matching ranges let the join run without shuffling its inputs.
        if task_count == Some(2) {
            assert_snapshot!(distributed_plan, @r#"
            ┌───── DistributedExec
            │ SortPreservingMergeExec: [env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST, service@3 ASC NULLS LAST, host@4 ASC NULLS LAST, value@1 ASC NULLS LAST]
            │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=2, partitions=4
              │ SortExec: expr=[env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST, service@3 ASC NULLS LAST, host@4 ASC NULLS LAST, value@1 ASC NULLS LAST], preserve_partitioning=[true]
              │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[timestamp@4, value@5, env@0, service@1, host@2]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(B)], 2), file_type=parquet
              │       t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(D)], 2), file_type=parquet
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B)], 2), file_type=parquet
              │       t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(D)], 2), file_type=parquet
              └──────────────────────────────────────────────────
            "#);
        } else if task_count == Some(3) {
            assert_snapshot!(distributed_plan, @r#"
            ┌───── DistributedExec
            │ SortPreservingMergeExec: [env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST, service@3 ASC NULLS LAST, host@4 ASC NULLS LAST, value@1 ASC NULLS LAST]
            │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=3, partitions=6
              │ SortExec: expr=[env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST, service@3 ASC NULLS LAST, host@4 ASC NULLS LAST, value@1 ASC NULLS LAST], preserve_partitioning=[true]
              │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[timestamp@4, value@5, env@0, service@1, host@2]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(B)], 2), file_type=parquet
              │       t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet], []]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(D)], 2), file_type=parquet
              │       t2: DataSourceExec: file_groups={2 groups: [[], [/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(D)], 2), file_type=parquet
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B)], 2), file_type=parquet
              │       t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet], []]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(D)], 2), file_type=parquet
              │       t2: DataSourceExec: file_groups={2 groups: [[], [/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(D)], 2), file_type=parquet
              └──────────────────────────────────────────────────
            "#);
        } else if task_count == Some(5) {
            assert_snapshot!(distributed_plan, @r#"
            ┌───── DistributedExec
            │ SortPreservingMergeExec: [env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST, service@3 ASC NULLS LAST, host@4 ASC NULLS LAST, value@1 ASC NULLS LAST]
            │   [Stage 1] => NetworkCoalesceExec: output_partitions=5, input_tasks=5
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=5, partitions=5
              │ SortExec: expr=[env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST, service@3 ASC NULLS LAST, host@4 ASC NULLS LAST, value@1 ASC NULLS LAST], preserve_partitioning=[true]
              │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[timestamp@4, value@5, env@0, service@1, host@2]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [], 1), file_type=parquet
              │       t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [], 1), file_type=parquet
              │       t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [], 1), file_type=parquet
              │       t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [], 1), file_type=parquet
              │       t4: DataSourceExec: file_groups={1 group: [[]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [], 1), file_type=parquet
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [], 1), file_type=parquet
              │       t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [], 1), file_type=parquet
              │       t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [], 1), file_type=parquet
              │       t3: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [], 1), file_type=parquet
              │       t4: DataSourceExec: file_groups={1 group: [[]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [], 1), file_type=parquet
              └──────────────────────────────────────────────────
            "#);
        } else {
            assert_snapshot!(distributed_plan, @r#"
            ┌───── DistributedExec
            │ SortPreservingMergeExec: [env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST, service@3 ASC NULLS LAST, host@4 ASC NULLS LAST, value@1 ASC NULLS LAST]
            │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=3, partitions=6
              │ SortExec: expr=[env@2 ASC NULLS LAST, timestamp@0 ASC NULLS LAST, service@3 ASC NULLS LAST, host@4 ASC NULLS LAST, value@1 ASC NULLS LAST], preserve_partitioning=[true]
              │   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@3, f_dkey@2)], projection=[timestamp@4, value@5, env@0, service@1, host@2]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(B)], 2), file_type=parquet
              │       t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(D)], 2), file_type=parquet
              │       t2: DataSourceExec: file_groups={2 groups: [[], [/testdata/join/parquet_multiple_files/dim/d_dkey=E/E0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=E/E1.parquet]]}, projection=[env, service, host, d_dkey], output_partitioning=Range([d_dkey@3 ASC NULLS LAST], [(E)], 2), file_type=parquet
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B)], 2), file_type=parquet
              │       t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(D)], 2), file_type=parquet
              │       t2: DataSourceExec: file_groups={2 groups: [[], [/testdata/join/parquet_multiple_files/fact/f_dkey=E/E0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=E/E1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=E/E2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(E)], 2), file_type=parquet
              └──────────────────────────────────────────────────
            "#);
        }
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
        register_tables(&distributed_ctx, &["A", "B", "C", "D"]).await?;
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
            │ RepartitionExec: partitioning=Hash([env@0, time_bin@1], 4), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[env@1 as env, time_bin@0 as time_bin], aggr=[avg(a.max_bin_value)]
            │     ProjectionExec: expr=[date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp)@1 as time_bin, env@2 as env, max(j.value)@3 as max_bin_value]
            │       AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey, date_bin(IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }, timestamp@2) as date_bin(IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 0, nanoseconds: 30000000000 }"),j.timestamp), env@1 as env], aggr=[max(j.value)]
            │         HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@2)], projection=[f_dkey@4, env@0, timestamp@2, value@3]
            │           FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
            │             DistributedLeafExec:
            │               t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[env, service, d_dkey], output_partitioning=Range([d_dkey@2 ASC NULLS LAST], [(B)], 2), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │               t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[env, service, d_dkey], output_partitioning=Range([d_dkey@2 ASC NULLS LAST], [(D)], 2), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │           DistributedLeafExec:
            │             t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(B)], 2), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │             t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[timestamp, value, f_dkey], output_partitioning=Range([f_dkey@2 ASC NULLS LAST], [(D)], 2), file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
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

    fn set_configs(ctx: &mut SessionContext) {
        ctx.set_distributed_route_task_handler(UrlEmitterRouteTaskHandler);
        let state = ctx.state_ref();
        let mut state = state.write();
        let options = state.config_mut().options_mut();
        options.execution.target_partitions = 2;
        options.optimizer.hash_join_single_partition_threshold = 0;
        options.optimizer.hash_join_single_partition_threshold_rows = 0;
    }

    async fn register_tables(ctx: &SessionContext, partition_keys: &[&str]) -> Result<()> {
        for (name, partition_column) in [("dim", "d_dkey"), ("fact", "f_dkey")] {
            let output_partitioning = Partitioning::Range(RangePartitioning::try_new(
                vec![col(partition_column).sort(true, false)],
                partition_keys[1..]
                    .iter()
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
            // Select keys by filename: the glob ignores Hive partition directories.
            let config = ListingTableConfig::new(ListingTableUrl::parse(format!(
                "testdata/join/parquet_multiple_files/{name}/[{}]*.parquet",
                partition_keys.join(""),
            ))?)
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
