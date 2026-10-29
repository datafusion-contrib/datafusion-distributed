from inline_snapshot import snapshot

from datafusion_distributed import DistributedSessionContext
from snapshot_utils import anonymize_snapshot


def test_execute_weather_table(
    weather_ctx: DistributedSessionContext,
):
    df = weather_ctx.sql(
        'SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY "count(*)"'
    )

    assert anonymize_snapshot(df.execution_plan().display_indent()) == snapshot("""\
DistributedExec
  SortPreservingMergeExec: [count(*)@0 ASC NULLS LAST]
    [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
      ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday]
        SortExec: expr=[count(Int64(1))@1 ASC NULLS LAST], preserve_partitioning=[true]
          AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
              RepartitionExec: partitioning=Hash([RainToday@0], 9), input_partitions=3
                AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                  DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
""")

    assert repr(df) == snapshot("""\
DataFrame()
+----------+-----------+
| count(*) | RainToday |
+----------+-----------+
| 66       | Yes       |
| 300      | No        |
+----------+-----------+\
""")
