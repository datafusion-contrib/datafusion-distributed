#[cfg(test)]
mod tests {
    use crate::common::TestQuery;
    use datafusion::common::Result;
    use datafusion_distributed::assert_snapshot;

    /// A partial aggregate updates a data source in the same stage.
    #[tokio::test]
    async fn local_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT MIN("MinTemp")
                FROM weather
                WHERE "RainToday" = 'Yes'
            "#,
        )
        .execute()
        .await?;
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ AggregateExec: mode=Final, gby=[], aggr=[min(weather.MinTemp)]
        │   CoalescePartitionsExec
        │     [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=6
          │ AggregateExec: mode=Partial, gby=[], aggr=[min(weather.MinTemp)]
          │   FilterExec: RainToday@1 = Yes, projection=[MinTemp@0]
          │     DistributedLeafExec:
          │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=RainToday@19 = Yes AND DynamicFilter [ MinTemp@0 < 4.3 ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= Yes AND Yes <= RainToday_max@1 AND MinTemp_null_count@5 != row_count@3 AND MinTemp_min@4 < 4.3, required_guarantees=[RainToday in (Yes)]
          │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp, RainToday], file_type=parquet, predicate=RainToday@19 = Yes AND DynamicFilter [ MinTemp@0 < -1.6 ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= Yes AND Yes <= RainToday_max@1 AND MinTemp_null_count@5 != row_count@3 AND MinTemp_min@4 < -1.6, required_guarantees=[RainToday in (Yes)]
          └──────────────────────────────────────────────────
        ");
        Ok(())
    }

    /// A partial aggregate does not yet update a data source across a shuffle.
    #[tokio::test]
    async fn remote_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT MIN(key)
                FROM (
                    SELECT DISTINCT "MinTemp" AS key
                    FROM weather
                )
            "#,
        )
        .execute()
        .await?;
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ AggregateExec: mode=Final, gby=[], aggr=[min(key)]
        │   CoalescePartitionsExec
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=3
          │ AggregateExec: mode=Partial, gby=[], aggr=[min(key)]
          │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp@0 as key], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp@0 as key], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }
}
