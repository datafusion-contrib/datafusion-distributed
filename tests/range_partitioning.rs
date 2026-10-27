#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::{Result, ScalarValue, SplitPoint};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::logical_expr::{Partitioning, RangePartitioning};
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionContext, col};
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::routing::UrlEmitterRouteTaskHandler;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExt, assert_snapshot, display_plan_ascii,
    };
    use std::sync::Arc;
    use test_case::test_case;

    #[test_case(&["A", "B", "C", "D"], Some(2); "even")]
    #[test_case(&["A", "B", "C", "D", "E"], None; "padding")]
    #[test_case(&["A", "B", "C", "D"], Some(5); "empty_task")]
    #[test_case(&["A", "B", "C", "D"], Some(1); "single_task")]
    #[tokio::test]
    async fn copartitioned_scans(keys: &[&str], tasks: Option<usize>) -> Result<()> {
        let sql = r#"
            SELECT f.f_dkey, COUNT(*) AS n
            FROM dim d JOIN fact f ON d.d_dkey = f.f_dkey
            GROUP BY f.f_dkey ORDER BY f.f_dkey
        "#;
        let baseline = SessionContext::new();
        let (mut distributed, _guard, _) =
            start_localhost_context(tasks.unwrap_or(4), DefaultSessionBuilder).await;
        configure(&mut distributed, tasks);
        for ctx in [&baseline, &distributed] {
            ctx.sql("SET datafusion.optimizer.enable_dynamic_filter_pushdown = false")
                .await?;
            register_tables(ctx, keys).await?;
        }
        let expected = baseline.sql(sql).await?.collect().await?;
        let plan = distributed.sql(sql).await?.create_physical_plan().await?;
        let display = display_plan_ascii(plan.as_ref(), false);
        let actual = collect(plan, distributed.task_ctx()).await?;
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            pretty_format_batches(&expected)?.to_string()
        );
        assert!(!display.contains("NetworkShuffleExec"), "{display}");
        match tasks {
            Some(2) => assert_snapshot!(display, @r"
            ┌───── DistributedExec
            │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST]
            │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2, partitioning=Range([f_dkey@0 ASC NULLS LAST], [(B), (C), (D)], 4)
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=2, partitions=4
              │ ProjectionExec: expr=[f_dkey@0 as f_dkey, count(Int64(1))@1 as n]
              │   SortExec: expr=[f_dkey@0 ASC NULLS LAST], preserve_partitioning=[true]
              │     AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey], aggr=[count(Int64(1))]
              │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@0, f_dkey@0)], projection=[f_dkey@1]
              │         DistributedLeafExec:
              │           t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [(B)], 2, end=(C)), file_type=parquet
              │           t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [(D)], 2, start=(C)), file_type=parquet
              │         DistributedLeafExec:
              │           t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(B)], 2, end=(C)), file_type=parquet
              │           t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(D)], 2, start=(C)), file_type=parquet
              └──────────────────────────────────────────────────
            "),
            None => assert_snapshot!(display, @r"
            ┌───── DistributedExec
            │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST]
            │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3, partitioning=Range([f_dkey@0 ASC NULLS LAST], [(B), (C), (D), (E), (E)], 6)
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=3, partitions=6
              │ ProjectionExec: expr=[f_dkey@0 as f_dkey, count(Int64(1))@1 as n]
              │   SortExec: expr=[f_dkey@0 ASC NULLS LAST], preserve_partitioning=[true]
              │     AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey], aggr=[count(Int64(1))]
              │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@0, f_dkey@0)], projection=[f_dkey@1]
              │         DistributedLeafExec:
              │           t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [(B)], 2, end=(C)), file_type=parquet
              │           t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [(D)], 2, start=(C), end=(E)), file_type=parquet
              │           t2: DataSourceExec: file_groups={2 groups: [[], [/testdata/join/parquet_multiple_files/dim/d_dkey=E/E0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=E/E1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [(E)], 2, start=(E)), file_type=parquet
              │         DistributedLeafExec:
              │           t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(B)], 2, end=(C)), file_type=parquet
              │           t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(D)], 2, start=(C), end=(E)), file_type=parquet
              │           t2: DataSourceExec: file_groups={2 groups: [[], [/testdata/join/parquet_multiple_files/fact/f_dkey=E/E0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=E/E1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=E/E2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(E)], 2, start=(E)), file_type=parquet
              └──────────────────────────────────────────────────
            "),
            Some(5) => assert_snapshot!(display, @r"
            ┌───── DistributedExec
            │ SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST]
            │   [Stage 1] => NetworkCoalesceExec: output_partitions=5, input_tasks=5, partitioning=Range([f_dkey@0 ASC NULLS LAST], [(B), (C), (D), (D)], 5)
            └──────────────────────────────────────────────────
              ┌───── Stage 1 ── tasks=5, partitions=5
              │ ProjectionExec: expr=[f_dkey@0 as f_dkey, count(Int64(1))@1 as n]
              │   SortExec: expr=[f_dkey@0 ASC NULLS LAST], preserve_partitioning=[true]
              │     AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey], aggr=[count(Int64(1))]
              │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@0, f_dkey@0)], projection=[f_dkey@1]
              │         DistributedLeafExec:
              │           t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [], 1, end=(B)), file_type=parquet
              │           t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [], 1, start=(B), end=(C)), file_type=parquet
              │           t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [], 1, start=(C), end=(D)), file_type=parquet
              │           t3: DataSourceExec: file_groups={1 group: [[]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [], 1, start=(D), end=(D)), file_type=parquet
              │           t4: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [], 1, start=(D)), file_type=parquet
              │         DistributedLeafExec:
              │           t0: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [], 1, end=(B)), file_type=parquet
              │           t1: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [], 1, start=(B), end=(C)), file_type=parquet
              │           t2: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [], 1, start=(C), end=(D)), file_type=parquet
              │           t3: DataSourceExec: file_groups={1 group: [[]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [], 1, start=(D), end=(D)), file_type=parquet
              │           t4: DataSourceExec: file_groups={1 group: [[/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [], 1, start=(D)), file_type=parquet
              └──────────────────────────────────────────────────
            "),
            Some(1) => assert_snapshot!(display, @r"
            SortPreservingMergeExec: [f_dkey@0 ASC NULLS LAST]
              ProjectionExec: expr=[f_dkey@0 as f_dkey, count(Int64(1))@1 as n]
                SortExec: expr=[f_dkey@0 ASC NULLS LAST], preserve_partitioning=[true]
                  AggregateExec: mode=SinglePartitioned, gby=[f_dkey@0 as f_dkey], aggr=[count(Int64(1))]
                    HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@0, f_dkey@0)], projection=[f_dkey@1]
                      DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[d_dkey], output_partitioning=Range([d_dkey@0 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet
                      DataSourceExec: file_groups={4 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(B), (C), (D)], 4), file_type=parquet
            "),
            _ => unreachable!(),
        }
        Ok(())
    }

    fn configure(ctx: &mut SessionContext, tasks: Option<usize>) {
        ctx.set_distributed_route_task_handler(UrlEmitterRouteTaskHandler);
        if let Some(tasks) = tasks {
            ctx.set_distributed_desired_task_count_handler(tasks);
        }
        let state = ctx.state_ref();
        let mut state = state.write();
        let options = state.config_mut().options_mut();
        options.execution.target_partitions = 2;
        options.optimizer.hash_join_single_partition_threshold = 0;
        options.optimizer.hash_join_single_partition_threshold_rows = 0;
    }

    async fn register_tables(ctx: &SessionContext, keys: &[&str]) -> Result<()> {
        for (name, key) in [("dim", "d_dkey"), ("fact", "f_dkey")] {
            let range = RangePartitioning::try_new(
                vec![col(key).sort(true, false)],
                keys[1..]
                    .iter()
                    .map(|value| SplitPoint::new(vec![ScalarValue::from(*value)]))
                    .collect(),
            )?;
            let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_table_partition_cols(vec![(key.to_owned(), DataType::Utf8)])
                .with_output_partitioning(Some(Partitioning::Range(range)));
            let table = ListingTableConfig::new(ListingTableUrl::parse(format!(
                "testdata/join/parquet_multiple_files/{name}/[{}]*.parquet",
                keys.join(""),
            ))?)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await?;
            ctx.register_table(name, Arc::new(ListingTable::try_new(table)?))?;
        }
        Ok(())
    }
}
