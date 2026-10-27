#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::datatypes::DataType;
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::config::ConfigOptions;
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::common::{Result, ScalarValue, SplitPoint};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::execution::SessionStateBuilder;
    use datafusion::logical_expr::{Partitioning, RangePartitioning};
    use datafusion::physical_expr::{
        LexOrdering, Partitioning as PhysicalPartitioning, PhysicalSortExpr,
        RangePartitioning as PhysicalRangePartitioning,
    };
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
    use datafusion::physical_plan::repartition::RepartitionExec;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
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

    #[test_case(2, false, false; "many_to_many")]
    #[test_case(3, false, false; "padding")]
    #[test_case(6, false, false; "empty_consumer")]
    #[test_case(2, true, false; "adaptive")]
    #[test_case(3, false, true; "shuffle_to_copartitioned_scan")]
    #[test_case(3, true, true; "adaptive_copartitioned_scan")]
    #[tokio::test]
    async fn range_shuffle_join(tasks: usize, adaptive: bool, one_sided: bool) -> Result<()> {
        let sql = r#"
            SELECT d.k, COUNT(*) AS n
            FROM dim_view d JOIN fact_view f ON d.k = f.k
            GROUP BY d.k ORDER BY d.k
        "#;
        let keys = &["A", "B", "C", "D", "E"];
        let baseline = SessionContext::new();
        let (mut distributed, _guard, _) =
            start_localhost_context(tasks.max(4), DefaultSessionBuilder).await;
        configure(&mut distributed, Some(tasks));
        distributed.set_distributed_dynamic_task_count(adaptive)?;
        let state = SessionStateBuilder::new_from_existing(distributed.state())
            .with_physical_optimizer_rule(Arc::new(RangeShuffleRule))
            .build();
        *distributed.state_ref().write() = state;
        for ctx in [&baseline, &distributed] {
            ctx.sql("SET datafusion.optimizer.enable_dynamic_filter_pushdown = false")
                .await?;
            register_tables(ctx, keys).await?;
            for (name, key) in [("dim", "d_dkey"), ("fact", "f_dkey")] {
                let view = if one_sided && name == "fact" {
                    ctx.sql(&format!("SELECT {key} AS k FROM {name}")).await?
                } else {
                    // Rotate keys across range boundaries, so rows must change tasks.
                    ctx.sql(&format!(
                        "SELECT TRANSLATE({key}, 'ABCDE', 'CDEAB') AS k FROM {name}"
                    ))
                    .await?
                };
                ctx.register_table(format!("{name}_view"), view.into_view())?;
            }
        }
        let expected = baseline.sql(sql).await?.collect().await?;
        let plan = distributed.sql(sql).await?.create_physical_plan().await?;
        let actual = collect(Arc::clone(&plan), distributed.task_ctx()).await?;
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            pretty_format_batches(&expected)?.to_string()
        );
        assert_eq!(
            actual.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            keys.len()
        );
        let display = display_plan_ascii(plan.as_ref(), false);
        assert_eq!(
            display.matches("NetworkShuffleExec").count(),
            if one_sided { 1 } else { 2 },
            "{display}"
        );
        assert!(!display.contains("partitioning=Hash("), "{display}");
        if tasks == 3 && !one_sided {
            assert_snapshot!(display, @r"
            ┌───── DistributedExec
            │ SortPreservingMergeExec: [k@0 ASC NULLS LAST]
            │   [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=3, partitioning=Range([k@0 ASC NULLS LAST], [(B), (C), (D), (E), (E)], 6)
            └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=3, partitions=6
              │ ProjectionExec: expr=[k@0 as k, count(Int64(1))@1 as n]
              │   SortExec: expr=[k@0 ASC NULLS LAST], preserve_partitioning=[true]
              │     AggregateExec: mode=SinglePartitioned, gby=[k@0 as k], aggr=[count(Int64(1))]
              │       HashJoinExec: mode=Partitioned, join_type=Inner, on=[(k@0, k@0)], projection=[k@0]
              │         [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=3, partitioning=Range([k@0 ASC NULLS LAST], [(B)], 2, end=(C))
              │         [Stage 2] => NetworkShuffleExec: output_partitions=2, input_tasks=3, partitioning=Range([k@0 ASC NULLS LAST], [(B)], 2, end=(C))
              └──────────────────────────────────────────────────
                ┌───── Stage 1 ── tasks=3, partitions=18
                │ RepartitionExec: partitioning=Range([k@0 ASC NULLS LAST], [(B), (C), (D), (E), (E)], 6), input_partitions=2
                │   DistributedLeafExec:
                │     t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet]]}, projection=[translate(d_dkey@3, ABCDE, CDEAB) as k], output_partitioning=UnknownPartitioning(2), file_type=parquet
                │     t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet], [/testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet]]}, projection=[translate(d_dkey@3, ABCDE, CDEAB) as k], output_partitioning=UnknownPartitioning(2), file_type=parquet
                │     t2: DataSourceExec: file_groups={2 groups: [[], [/testdata/join/parquet_multiple_files/dim/d_dkey=E/E0.parquet, /testdata/join/parquet_multiple_files/dim/d_dkey=E/E1.parquet]]}, projection=[translate(d_dkey@3, ABCDE, CDEAB) as k], output_partitioning=UnknownPartitioning(2), file_type=parquet
                └──────────────────────────────────────────────────
                ┌───── Stage 2 ── tasks=3, partitions=18
                │ RepartitionExec: partitioning=Range([k@0 ASC NULLS LAST], [(B), (C), (D), (E), (E)], 6), input_partitions=2
                │   DistributedLeafExec:
                │     t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet]]}, projection=[translate(f_dkey@2, ABCDE, CDEAB) as k], output_partitioning=UnknownPartitioning(2), file_type=parquet
                │     t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet], [/testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet]]}, projection=[translate(f_dkey@2, ABCDE, CDEAB) as k], output_partitioning=UnknownPartitioning(2), file_type=parquet
                │     t2: DataSourceExec: file_groups={2 groups: [[], [/testdata/join/parquet_multiple_files/fact/f_dkey=E/E0.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=E/E1.parquet, /testdata/join/parquet_multiple_files/fact/f_dkey=E/E2.parquet]]}, projection=[translate(f_dkey@2, ABCDE, CDEAB) as k], output_partitioning=UnknownPartitioning(2), file_type=parquet
                └──────────────────────────────────────────────────
            ");
        }
        Ok(())
    }

    // DataFusion 55 chooses hash repartitions for SQL joins. Model an embedding's range
    // planner after its optimizer, which otherwise removes explicit range repartitions.
    #[derive(Debug)]
    struct RangeShuffleRule;

    impl PhysicalOptimizerRule for RangeShuffleRule {
        fn optimize(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            config: &ConfigOptions,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let plan = plan
                .transform_up(|plan| {
                    let Some(repartition) = plan.downcast_ref::<RepartitionExec>() else {
                        return Ok(Transformed::no(plan));
                    };
                    let PhysicalPartitioning::Hash(keys, _) = repartition.partitioning() else {
                        return Ok(Transformed::no(plan));
                    };
                    let partitioning =
                        PhysicalPartitioning::Range(PhysicalRangePartitioning::try_new(
                            LexOrdering::new(keys.iter().map(|key| {
                                PhysicalSortExpr::new_default(Arc::clone(key)).nulls_last()
                            }))
                            .unwrap(),
                            ["B", "C", "D", "E"]
                                .into_iter()
                                .map(|key| SplitPoint::new(vec![ScalarValue::from(key)]))
                                .collect(),
                        )?);
                    let input = Arc::clone(repartition.input());
                    let plan: Arc<dyn ExecutionPlan> = if input.output_partitioning()
                        == &partitioning
                    {
                        input
                    } else {
                        Arc::new(
                            RepartitionExec::try_new(input, partitioning)?.with_preserve_order(),
                        )
                    };
                    Ok(Transformed::yes(plan))
                })?
                .data;
            SanityCheckPlan::new().optimize(plan, config)
        }

        fn name(&self) -> &str {
            "range_shuffle_test"
        }

        fn schema_check(&self) -> bool {
            true
        }
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
