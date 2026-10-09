#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{CsvReadOptions, SessionContext, col};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, assert_snapshot, display_plan_ascii,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use uuid::Uuid;

    const MULTILINE_CSV: &str = r#"value
"0first
0middle
0last"
"1first
1middle
1last"
"2first
2middle
2last"
"3first
3middle
3last"
"4first
4middle
4last"
"5first
5middle
5last"
"6first
6middle
6last"
"#;

    // Each file is sorted, but their value ranges run opposite to file-name order.
    const SORTED_CSV: &[&str] = &[
        "value
8
8
9
9
10
10
11
11
12
12
13
13
14
14
15
15
",
        "value
0
0
1
1
2
2
3
3
4
4
5
5
6
6
7
7
",
    ];

    #[tokio::test]
    async fn sorted_csv_with_embedded_newlines_static() -> Result<()> {
        let (plan, result) = CsvTest::new(&[MULTILINE_CSV])
            .newlines_in_values(true)
            .query("SELECT count(*) AS n FROM records")
            .await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as n]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=4
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={1 group: [[testdata/sorted_csv/0.csv:<int>..<int>]]}, file_type=csv, has_header=true
          │       t1: DataSourceExec: file_groups={1 group: [[testdata/sorted_csv/0.csv:<int>..<int>]]}, file_type=csv, has_header=true
          └──────────────────────────────────────────────────
        ");
        assert_snapshot!(result, @r"
        +---+
        | n |
        +---+
        | 7 |
        +---+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn sorted_csv_with_embedded_newlines_adaptive() -> Result<()> {
        let (plan, result) = CsvTest::new(&[MULTILINE_CSV])
            .newlines_in_values(true)
            .adaptive(true)
            .query("SELECT count(*) AS n FROM records")
            .await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as n]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=4
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={1 group: [[testdata/sorted_csv/0.csv:<int>..<int>]]}, file_type=csv, has_header=true
          │       t1: DataSourceExec: file_groups={1 group: [[testdata/sorted_csv/0.csv:<int>..<int>]]}, file_type=csv, has_header=true
          └──────────────────────────────────────────────────
        ");
        assert_snapshot!(result, @r"
        +---+
        | n |
        +---+
        | 7 |
        +---+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn sorted_csv_aggregation_static_direct_shuffle() -> Result<()> {
        let (plan, result) = CsvTest::new(SORTED_CSV)
            .query("SELECT value, count(*) AS n FROM records GROUP BY value ORDER BY value")
            .await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [value@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=2
          │ ProjectionExec: expr=[value@0 as value, count(Int64(1))@1 as n]
          │   AggregateExec: mode=FinalPartitioned, gby=[value@0 as value], aggr=[count(Int64(1))], ordering_mode=Sorted
          │     [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2, sort_exprs=[value@0 ASC NULLS LAST]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=4
            │ RepartitionExec: partitioning=Hash([value@0], 4), input_partitions=2, preserve_order=true, sort_exprs=value@0 ASC NULLS LAST
            │   AggregateExec: mode=Partial, gby=[value@0 as value], aggr=[count(Int64(1))], ordering_mode=Sorted
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], output_ordering=[value@0 ASC NULLS LAST], file_type=csv, has_header=true
            │       t1: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], output_ordering=[value@0 ASC NULLS LAST], file_type=csv, has_header=true
            └──────────────────────────────────────────────────
        ");
        assert_snapshot!(result, @r"
        +-------+---+
        | value | n |
        +-------+---+
        | 0     | 2 |
        | 1     | 2 |
        | 2     | 2 |
        | 3     | 2 |
        | 4     | 2 |
        | 5     | 2 |
        | 6     | 2 |
        | 7     | 2 |
        | 8     | 2 |
        | 9     | 2 |
        | 10    | 2 |
        | 11    | 2 |
        | 12    | 2 |
        | 13    | 2 |
        | 14    | 2 |
        | 15    | 2 |
        +-------+---+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn sorted_csv_aggregation_adaptive_direct_shuffle() -> Result<()> {
        let (plan, result) = CsvTest::new(SORTED_CSV)
            .adaptive(true)
            .query("SELECT value, count(*) AS n FROM records GROUP BY value ORDER BY value")
            .await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [value@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=2
          │ ProjectionExec: expr=[value@0 as value, count(Int64(1))@1 as n]
          │   AggregateExec: mode=FinalPartitioned, gby=[value@0 as value], aggr=[count(Int64(1))], ordering_mode=Sorted
          │     [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2, sort_exprs=[value@0 ASC NULLS LAST]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=4
            │ RepartitionExec: partitioning=Hash([value@0], 4), input_partitions=2, preserve_order=true, sort_exprs=value@0 ASC NULLS LAST
            │   SamplerExec: partitions=2
            │     AggregateExec: mode=Partial, gby=[value@0 as value], aggr=[count(Int64(1))], ordering_mode=Sorted
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], output_ordering=[value@0 ASC NULLS LAST], file_type=csv, has_header=true
            │         t1: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], output_ordering=[value@0 ASC NULLS LAST], file_type=csv, has_header=true
            └──────────────────────────────────────────────────
        ");
        assert_snapshot!(result, @r"
        +-------+---+
        | value | n |
        +-------+---+
        | 0     | 2 |
        | 1     | 2 |
        | 2     | 2 |
        | 3     | 2 |
        | 4     | 2 |
        | 5     | 2 |
        | 6     | 2 |
        | 7     | 2 |
        | 8     | 2 |
        | 9     | 2 |
        | 10    | 2 |
        | 11    | 2 |
        | 12    | 2 |
        | 13    | 2 |
        | 14    | 2 |
        | 15    | 2 |
        +-------+---+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn sorted_csv_aggregation_static_two_step_shuffle() -> Result<()> {
        let (plan, result) = CsvTest::new(SORTED_CSV)
            .two_step_shuffle(true)
            .query("SELECT value, count(*) AS n FROM records GROUP BY value ORDER BY value")
            .await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [value@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=2
          │ ProjectionExec: expr=[value@0 as value, count(Int64(1))@1 as n]
          │   AggregateExec: mode=FinalPartitioned, gby=[value@0 as value], aggr=[count(Int64(1))], ordering_mode=Sorted
          │     RepartitionExec: partitioning=Hash([value@0], 2), input_partitions=2, preserve_order=true, sort_exprs=value@0 ASC NULLS LAST
          │       [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2, sort_exprs=[value@0 ASC NULLS LAST]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=2
            │ RepartitionExec: partitioning=Hash([value@0, 5871781006564002453], 2), input_partitions=2, preserve_order=true, sort_exprs=value@0 ASC NULLS LAST
            │   AggregateExec: mode=Partial, gby=[value@0 as value], aggr=[count(Int64(1))], ordering_mode=Sorted
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], output_ordering=[value@0 ASC NULLS LAST], file_type=csv, has_header=true
            │       t1: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], output_ordering=[value@0 ASC NULLS LAST], file_type=csv, has_header=true
            └──────────────────────────────────────────────────
        ");
        assert_snapshot!(result, @r"
        +-------+---+
        | value | n |
        +-------+---+
        | 0     | 2 |
        | 1     | 2 |
        | 2     | 2 |
        | 3     | 2 |
        | 4     | 2 |
        | 5     | 2 |
        | 6     | 2 |
        | 7     | 2 |
        | 8     | 2 |
        | 9     | 2 |
        | 10    | 2 |
        | 11    | 2 |
        | 12    | 2 |
        | 13    | 2 |
        | 14    | 2 |
        | 15    | 2 |
        +-------+---+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn sorted_csv_aggregation_adaptive_two_step_shuffle() -> Result<()> {
        let (plan, result) = CsvTest::new(SORTED_CSV)
            .adaptive(true)
            .two_step_shuffle(true)
            .query("SELECT value, count(*) AS n FROM records GROUP BY value ORDER BY value")
            .await?;
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [value@0 ASC NULLS LAST]
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=2
          │ ProjectionExec: expr=[value@0 as value, count(Int64(1))@1 as n]
          │   AggregateExec: mode=FinalPartitioned, gby=[value@0 as value], aggr=[count(Int64(1))], ordering_mode=Sorted
          │     RepartitionExec: partitioning=Hash([value@0], 2), input_partitions=2, preserve_order=true, sort_exprs=value@0 ASC NULLS LAST
          │       [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2, sort_exprs=[value@0 ASC NULLS LAST]
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=2
            │ RepartitionExec: partitioning=Hash([value@0, 5871781006564002453], 2), input_partitions=2, preserve_order=true, sort_exprs=value@0 ASC NULLS LAST
            │   SamplerExec: partitions=2
            │     AggregateExec: mode=Partial, gby=[value@0 as value], aggr=[count(Int64(1))], ordering_mode=Sorted
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], output_ordering=[value@0 ASC NULLS LAST], file_type=csv, has_header=true
            │         t1: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], output_ordering=[value@0 ASC NULLS LAST], file_type=csv, has_header=true
            └──────────────────────────────────────────────────
        ");
        assert_snapshot!(result, @r"
        +-------+---+
        | value | n |
        +-------+---+
        | 0     | 2 |
        | 1     | 2 |
        | 2     | 2 |
        | 3     | 2 |
        | 4     | 2 |
        | 5     | 2 |
        | 6     | 2 |
        | 7     | 2 |
        | 8     | 2 |
        | 9     | 2 |
        | 10    | 2 |
        | 11    | 2 |
        | 12    | 2 |
        | 13    | 2 |
        | 14    | 2 |
        | 15    | 2 |
        +-------+---+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn sorted_csv_files_with_unordered_ranges() -> Result<()> {
        let (plan, result) = CsvTest::new(&[
            r#"value
                7
                8
                9
            "#,
            r#"value
                0
            "#,
            r#"value
                1
            "#,
        ])
        .query("SELECT value FROM records ORDER BY value")
        .await?;
        // A task must not acquire an ordering that the other task cannot guarantee.
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [value@0 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=4
          │ SortExec: expr=[value@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/1.csv:<int>..<int>]]}, projection=[value], file_type=csv, has_header=true
          │     t1: DataSourceExec: file_groups={2 groups: [[testdata/sorted_csv/0.csv:<int>..<int>], [testdata/sorted_csv/2.csv:<int>..<int>]]}, projection=[value], file_type=csv, has_header=true
          └──────────────────────────────────────────────────
        ");
        assert_snapshot!(result, @r"
        +-------+
        | value |
        +-------+
        | 0     |
        | 1     |
        | 7     |
        | 8     |
        | 9     |
        +-------+
        ");
        Ok(())
    }

    struct CsvTest<'a> {
        bodies: &'a [&'a str],
        newlines_in_values: bool,
        adaptive: bool,
        two_step_shuffle: bool,
    }

    impl<'a> CsvTest<'a> {
        fn new(bodies: &'a [&'a str]) -> Self {
            Self {
                bodies,
                newlines_in_values: false,
                adaptive: false,
                two_step_shuffle: false,
            }
        }

        fn newlines_in_values(mut self, enabled: bool) -> Self {
            self.newlines_in_values = enabled;
            self
        }

        fn adaptive(mut self, enabled: bool) -> Self {
            self.adaptive = enabled;
            self
        }

        fn two_step_shuffle(mut self, enabled: bool) -> Self {
            self.two_step_shuffle = enabled;
            self
        }

        async fn query(self, sql: &str) -> Result<(String, String)> {
            let (ctx, dir) = self.context().await?;
            let plan = ctx.sql(sql).await?.create_physical_plan().await?;
            assert!(plan.is::<DistributedExec>());
            let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            let plan = display_plan_ascii(plan.as_ref(), false).replace(
                dir.to_str().unwrap().trim_start_matches('/'),
                "testdata/sorted_csv",
            );
            fs::remove_dir_all(dir)?;
            Ok((plan, pretty_format_batches(&batches)?.to_string()))
        }

        async fn context(&self) -> Result<(SessionContext, PathBuf)> {
            let mut ctx =
                start_in_memory_context(2, DefaultSessionBuilder)
                    .await
                    .with_distributed_file_scan_config_bytes_per_partition(1)?
                    .with_distributed_dynamic_bytes_per_partition(1)?
                    .with_distributed_two_step_shuffle_fanout_threshold(
                        if self.two_step_shuffle { 1 } else { usize::MAX },
                    )?;
            ctx.set_distributed_dynamic_task_count(self.adaptive)?;
            ctx.sql("SET datafusion.execution.target_partitions = 2")
                .await?;
            // Exercise merging across batches, while letting consumers trust scan ordering.
            ctx.sql("SET datafusion.execution.batch_size = 2").await?;
            ctx.sql("SET datafusion.optimizer.prefer_existing_sort = true")
                .await?;

            let dir = std::env::temp_dir().join(format!("sorted_csv_{}", Uuid::new_v4()));
            fs::create_dir_all(&dir)?;
            for (i, body) in self.bodies.iter().enumerate() {
                fs::write(dir.join(format!("{i}.csv")), body)?;
            }
            ctx.register_csv(
                "records",
                dir.to_str().unwrap(),
                CsvReadOptions::new()
                    .has_header(true)
                    .newlines_in_values(self.newlines_in_values)
                    .file_sort_order(vec![vec![col("value").sort(true, false)]]),
            )
            .await?;
            Ok((ctx, dir))
        }
    }
}
