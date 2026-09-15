#[cfg(test)]
mod tests {
    use crate::common::TestQuery;
    use datafusion::common::Result;
    use datafusion_distributed::{assert_snapshot, test_utils::insta::settings};

    /// A TopK SortExec applies dynamic filters to local data sources.
    #[tokio::test]
    async fn local_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT "MinTemp"
                FROM weather
                ORDER BY "MinTemp" DESC
                LIMIT 10
            "#,
        )
        .with_expected_rows(10)
        .execute()
        .await?;
        let mut settings = settings();
        settings.add_filter(
            r"(DynamicFilter \[[^\]\n]*? > )-?\d+(?:\.\d+)?( \])",
            "${1}<runtime>${2}",
        );
        settings.add_filter(r"(_max@\d+ > )-?\d+(?:\.\d+)?", "${1}<runtime>");
        settings.bind(|| assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [MinTemp@0 DESC], fetch=10
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=6
          │ SortExec: TopK(fetch=10), expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   DistributedLeafExec:
          │     t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ MinTemp@0 IS NULL OR MinTemp@0 > <runtime> ], dynamic_rg_pruning=eligible, pruning_predicate=MinTemp_null_count@0 > 0 OR MinTemp_null_count@0 != row_count@2 AND MinTemp_max@1 > <runtime>, required_guarantees=[]
          │     t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp], file_type=parquet, predicate=DynamicFilter [ MinTemp@0 IS NULL OR MinTemp@0 > <runtime> ], dynamic_rg_pruning=eligible, pruning_predicate=MinTemp_null_count@0 > 0 OR MinTemp_null_count@0 != row_count@2 AND MinTemp_max@1 > <runtime>, required_guarantees=[]
          └──────────────────────────────────────────────────
        "));
        Ok(())
    }

    /// A TopK sort does not yet update dynamic filters to remote consumers.
    #[tokio::test]
    async fn remote_dynamic_filters() -> Result<()> {
        let display = TestQuery::new(
            r#"
                SELECT key
                FROM (
                    SELECT DISTINCT "MinTemp" AS key
                    FROM weather
                )
                ORDER BY key DESC
                LIMIT 10
            "#,
        )
        .with_expected_rows(10)
        .execute()
        .await?;
        assert_snapshot!(display, @"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [key@0 DESC], fetch=10
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=6, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── tasks=2, partitions=3
          │ SortExec: TopK(fetch=10), expr=[key@0 DESC], preserve_partitioning=[true]
          │   AggregateExec: mode=FinalPartitioned, gby=[key@0 as key], aggr=[], lim=[10]
          │     [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── tasks=2, partitions=6
            │ RepartitionExec: partitioning=Hash([key@0], 6), input_partitions=3
            │   AggregateExec: mode=Partial, gby=[key@0 as key], aggr=[], lim=[10]
            │     DistributedLeafExec:
            │       t0: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000000.parquet:<int>..<int>, /testdata/weather/result-000001.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp@0 as key], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            │       t1: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet:<int>..<int>], [/testdata/weather/result-000001.parquet:<int>..<int>, /testdata/weather/result-000002.parquet:<int>..<int>], [/testdata/weather/result-000002.parquet:<int>..<int>]]}, projection=[MinTemp@0 as key], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            └──────────────────────────────────────────────────
        ");
        Ok(())
    }
}
