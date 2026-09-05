#[cfg(test)]
mod tests {
    use crate::common::{TestQuery, execute_range_partitioned_query};
    use datafusion::common::Result;
    use datafusion_distributed::assert_snapshot;

    /// A Partitioned HashJoinExec propagates dynamic filters to local consumers.
    #[tokio::test]
    async fn local_dynamic_filters() -> Result<()> {
        let display = execute_range_partitioned_query(
            r#"
                SELECT d.env, COUNT(*) AS n
                FROM dim d
                JOIN fact f ON d.d_dkey = f.f_dkey
                WHERE d.service = 'log'
                GROUP BY d.env
            "#,
            2,
        )
        .await?;
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
            │         t0: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/fact/f_dkey=A/data0.parquet], [/testdata/join/parquet/fact/f_dkey=C/data0.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(C)], 2), file_type=parquet, predicate=DynamicFilter [ f_dkey@2 >= A AND f_dkey@2 <= A AND f_dkey@2 IN (SET) ([<values>]) ], dynamic_rg_pruning=eligible, pruning_predicate=f_dkey_null_count@1 != row_count@2 AND f_dkey_max@0 >= A AND f_dkey_null_count@1 != row_count@2 AND f_dkey_min@3 <= A AND f_dkey_null_count@1 != row_count@2 AND f_dkey_min@3 <= A AND A <= f_dkey_max@0, required_guarantees=[f_dkey in (A)]
            │         t1: DataSourceExec: file_groups={2 groups: [[/testdata/join/parquet/fact/f_dkey=B/data0.parquet], [/testdata/join/parquet/fact/f_dkey=D/data0.parquet]]}, projection=[f_dkey], output_partitioning=Range([f_dkey@0 ASC NULLS LAST], [(C)], 2), file_type=parquet, predicate=DynamicFilter [ f_dkey@2 >= B AND f_dkey@2 <= B AND f_dkey@2 IN (SET) ([<values>]) ], dynamic_rg_pruning=eligible, pruning_predicate=f_dkey_null_count@1 != row_count@2 AND f_dkey_max@0 >= B AND f_dkey_null_count@1 != row_count@2 AND f_dkey_min@3 <= B AND f_dkey_null_count@1 != row_count@2 AND f_dkey_min@3 <= B AND B <= f_dkey_max@0, required_guarantees=[f_dkey in (B)]
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    /// A Partitioned HashJoinExec does not propagate dynamic filters to a remote consumer.
    #[tokio::test]
    async fn remote_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN weather probe ON build.key = probe."RainToday"
            "#,
        )
        .execute()
        .await?;
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
            │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[RainToday], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }
}
