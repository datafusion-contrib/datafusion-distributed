#[cfg(test)]
mod tests {
    use crate::common::{LOCAL_AND_REMOTE_UNION_QUERY, TestQuery, execute_range_partitioned_query};
    use datafusion::common::Result;
    use datafusion_distributed::assert_snapshot;
    use datafusion_distributed::test_utils::insta::insta::allow_duplicates;

    /// A Partitioned HashJoinExec propagates dynamic filters to local consumers.
    #[tokio::test]
    async fn local_dynamic_filters() -> Result<()> {
        let display = execute_range_partitioned_query(LOCAL_DYNAMIC_FILTER_QUERY, 2, false).await?;
        assert_local_dynamic_filters_plan(display);
        Ok(())
    }

    /// Colocated tasks must not share their task-local dynamic filters.
    #[tokio::test]
    async fn colocated_local_dynamic_filters() -> Result<()> {
        let display = execute_range_partitioned_query(LOCAL_DYNAMIC_FILTER_QUERY, 2, true).await?;
        assert_local_dynamic_filters_plan(display);
        Ok(())
    }

    const LOCAL_DYNAMIC_FILTER_QUERY: &str = r#"
        SELECT d.env, COUNT(*) AS n
        FROM dim d
        JOIN fact f ON d.d_dkey = f.f_dkey
        WHERE d.service = 'log'
        GROUP BY d.env
    "#;

    fn assert_local_dynamic_filters_plan(display: String) {
        // Each task in stage 1 produces a distinct dynamic filter and pushes it down
        // to the local consumers.
        allow_duplicates! {
            assert_snapshot!(display, @r"
        ┌───── DistributedExec
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=2
          │ ProjectionExec: expr=[env@0 as env, count(Int64(1))@1 as n]
          │   AggregateExec: mode=FinalPartitioned, gby=[env@0 as env], aggr=[count(Int64(1))]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=4
            │ RepartitionExec: partitioning=Hash([env@0], 4), input_partitions=2
            │   AggregateExec: mode=Partial, gby=[env@0 as env], aggr=[count(Int64(1))]
            │     HashJoinExec: mode=Partitioned, join_type=Inner, on=[(d_dkey@1, f_dkey@0)], projection=[env@0]
            │       FilterExec: service@1 = log, projection=[env@0, d_dkey@2]
            │         DistributedLeafExec:
            │           t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/dim/d_dkey=A/data0.parquet], [/testdata/join/parquet/dim/d_dkey=C/data0.parquet]]}, projection=[env, service, d_dkey], output_partitioning=Range([d_dkey@2 ASC NULLS LAST], [(C)], 2), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │           t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/dim/d_dkey=B/data0.parquet], [/testdata/join/parquet/dim/d_dkey=D/data0.parquet]]}, projection=[env, service, d_dkey], output_partitioning=Range([d_dkey@2 ASC NULLS LAST], [(C)], 2), file_type=parquet, predicate=service@1 = log, pruning_predicate=service_null_count@2 != row_count@3 AND service_min@0 <= log AND log <= service_max@1, required_guarantees=[service in (log)]
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(C)], 2), file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            │         t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(C)], 2), file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_1 ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        }
    }

    /// A Partitioned HashJoinExec propagates merged dynamic filters to a remote consumer.
    #[tokio::test]
    async fn remote_dynamic_filters() -> Result<()> {
        let display = remote_dynamic_filters_plan(false).await?;

        // Each join in stage 3 merges its filters at the coordinator which pushes
        // down one expression to the remote consumers in stage 2.
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=3
            │   DistributedLeafExec:
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn dynamic_planning_with_remote_consumers() -> Result<()> {
        let display = remote_dynamic_filters_plan(true).await?;

        // Each join in stage 3 merges its filters at the coordinator which pushes
        // down one expression to the remote consumers in stage 2.
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=3, input_tasks=1
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=1, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=3
            │ RepartitionExec: partitioning=Hash([key@0], 3), input_partitions=3
            │   SamplerExec: partitions=3
            │     AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │       DistributedLeafExec:
            │         t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            │         t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=3
            │ RepartitionExec: partitioning=Hash([RainToday@0], 3), input_partitions=3
            │   SamplerExec: partitions=3
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    /// Multiple consumers of one remote filter can remap it to different source columns.
    #[tokio::test]
    async fn remote_union_remaps_each_consumer() -> Result<()> {
        let display = TestQuery::new(
            r#"
                    WITH probe AS (
                        SELECT "RainToday" AS key FROM weather
                        UNION ALL
                        SELECT "WindGustDir" AS key FROM weather
                        UNION ALL
                        SELECT "WindDir9am" AS key FROM weather
                    )
                    SELECT COUNT(*)
                    FROM (
                        SELECT DISTINCT "RainToday" AS key
                        FROM weather
                    ) build
                    JOIN probe ON build.key = probe.key
                "#,
        )
        .with_one_task_per_leaf()
        .with_normalized_filter_hashes()
        .execute()
        .await?;

        // The each hash join in stage 3 propagates its dynamic filter to the coordindator where
        // it's merged and forwarded to the consumers in stage 2. Each consumer applies the dynamic
        // filter to a different column (RainToday, WindGustDir, WindDir9am), so the hashes are different but
        // the hashes with normalized column names match.
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 3] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 3 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, key@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=1
          │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=1, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 2 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=6
            │   DistributedUnionExec: t0:[c0, c2] t1:[c1]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0_normalized_hash_0 ], dynamic_rg_pruning=eligible
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_1_normalized_hash_0 ], dynamic_rg_pruning=eligible
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindDir9am@7 as key], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_2_normalized_hash_0 ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn remote_dynamic_filters_propagate_through_multiple_stages() -> Result<()> {
        let display = TestQuery::new(
            r#"
                    SELECT COUNT(*)
                    FROM (
                        SELECT DISTINCT "RainToday" AS key
                        FROM weather
                    ) build
                    JOIN (
                        SELECT probe."RainToday"
                        FROM (
                            SELECT "WindGustDir" AS key
                            FROM weather
                        ) nested_build
                        RIGHT JOIN weather probe
                            ON nested_build.key = probe."WindGustDir"
                    ) probe ON build.key = probe."RainToday"
                "#,
        )
        .execute()
        .await?;

        // The filters produced in stage 5 are merged into one filter and forwarded
        // to the consumers in stage 3.
        //
        // Note that the right join in stage 4 does not produce dynamic filters.
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 5] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 5 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, RainToday@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 4] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 4 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([RainToday@0], 6), input_partitions=3
            │   HashJoinExec: mode=Partitioned, join_type=Right, on=[(key@0, WindGustDir@0)], projection=[RainToday@2]
            │     [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            │     [Stage 3] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 2 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
              └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=2, partitions=6
              │ RepartitionExec: partitioning=Hash([WindGustDir@0], 6), input_partitions=3
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir, RainToday], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir, RainToday], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    #[tokio::test]
    async fn union_with_local_and_remote_probe_children() -> Result<()> {
        let display = TestQuery::new(LOCAL_AND_REMOTE_UNION_QUERY)
            .execute()
            .await?;

        // Complex test.
        // Dynamic filters from the stage 5 tasks are merged and forwarded to both c0 of t0 in stage
        // 4 and the consumers in stage 3. Dynamic filters from the join in stage 4 are also merged
        // and forwarded to the consumers in stage 3 where they are ANDed with the other filters.
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ ProjectionExec: expr=[count(Int64(1))@0 as count(*)]
        │   AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
        │     CoalescePartitionsExec
        │       [Stage 5] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 5 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
          │   HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, key@0)], projection=[]
          │     AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │       [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          │     [Stage 4] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet
            └──────────────────────────────────────────────────
            ┌───── Stage 4 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   DistributedUnionExec: t0:[c0] t1:[c1]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir@5 as key], file_type=parquet, predicate=DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
            │     ProjectionExec: expr=[WindGustDir@0 as key]
            │       HashJoinExec: mode=Partitioned, join_type=RightSemi, on=[(key@0, RainToday@1)], projection=[WindGustDir@0]
            │         AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
            │           [Stage 2] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            │         [Stage 3] => NetworkShuffleExec: output_partitions=3, input_tasks=2
            └──────────────────────────────────────────────────
              ┌───── Stage 2 ── tasks=2, partitions=3
              │ RepartitionExec: partitioning=Hash([key@0], 3), input_partitions=3
              │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
              │     DistributedLeafExec:
              │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday@19 as key], file_type=parquet
              └──────────────────────────────────────────────────
              ┌───── Stage 3 ── tasks=2, partitions=3
              │ RepartitionExec: partitioning=Hash([RainToday@1], 3), input_partitions=3
              │   DistributedLeafExec:
              │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir, RainToday], file_type=parquet, predicate=DynamicFilter [ expression_id_1_hash_1 ] AND DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
              │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[WindGustDir, RainToday], file_type=parquet, predicate=DynamicFilter [ expression_id_1_hash_1 ] AND DynamicFilter [ expression_id_0_hash_0 ], dynamic_rg_pruning=eligible
              └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    const REMOTE_DYNAMIC_FILTER_QUERY: &str = r#"
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT "RainToday" AS key
            FROM weather
        ) build
        JOIN weather probe ON build.key = probe."RainToday"
    "#;

    async fn remote_dynamic_filters_plan(dynamic_planning: bool) -> Result<String> {
        let query = TestQuery::new(REMOTE_DYNAMIC_FILTER_QUERY);
        let query = if dynamic_planning {
            query.with_dynamic_task_count()
        } else {
            query.expect_dynamic_filter_updates()
        };
        query.execute().await
    }
}
