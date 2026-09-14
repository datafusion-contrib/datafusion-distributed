import pyarrow as pa
import pyarrow.compute as pc
from datafusion import Accumulator, udaf, udf, udwf
from datafusion.user_defined import WindowEvaluator
from inline_snapshot import snapshot
from snapshot_utils import anonymize_snapshot

from datafusion_distributed import DistributedSessionContext


class SumAccumulator(Accumulator):
    def __init__(self) -> None:
        self.value = 0.0

    def state(self) -> list[pa.Scalar]:
        return [pa.scalar(self.value)]

    def update(self, values: pa.Array) -> None:
        self.value += pc.sum(values).as_py()

    def merge(self, states: list[pa.Array]) -> None:
        self.value += pc.sum(states[0]).as_py()

    def evaluate(self) -> pa.Scalar:
        return pa.scalar(self.value)


class RowNumber(WindowEvaluator):
    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        return pa.array(range(1, num_rows + 1), type=pa.uint64())


def test_python_udf_executes_on_distributed_workers(
    weather_ctx: DistributedSessionContext,
) -> None:
    is_rain = udf(
        lambda values: pc.equal(pc.cast(values, pa.string()), "Yes"),
        [pa.string_view()],
        pa.bool_(),
        volatility="immutable",
        name="is_rain",
    )
    weather_ctx.register_udf(is_rain)

    df = weather_ctx.sql(
        'SELECT count(*) AS rainy_days FROM weather WHERE is_rain("RainToday")'
    )

    assert anonymize_snapshot(df.execution_plan().display_indent()) == snapshot("""\
DistributedExec
  ProjectionExec: expr=[count(Int64(1))@0 as rainy_days]
    AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
      CoalescePartitionsExec
        [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
          AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
            FilterExec: is_rain(RainToday@0), projection=[]
              DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet, predicate=is_rain(RainToday@19)
""")
    assert repr(df) == snapshot("""\
DataFrame()
+------------+
| rainy_days |
+------------+
| 66         |
+------------+\
""")


def test_builtin_function_still_resolves_on_distributed_workers(
    weather_ctx: DistributedSessionContext,
) -> None:
    df = weather_ctx.sql('SELECT max(abs("MinTemp")) AS max_abs FROM weather')

    assert anonymize_snapshot(df.execution_plan().display_indent()) == snapshot("""\
DistributedExec
  ProjectionExec: expr=[max(abs(weather.MinTemp))@0 as max_abs]
    AggregateExec: mode=Final, gby=[], aggr=[max(abs(weather.MinTemp))]
      CoalescePartitionsExec
        [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
          AggregateExec: mode=Partial, gby=[], aggr=[max(abs(weather.MinTemp))]
            DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp], file_type=parquet
""")
    assert repr(df) == snapshot("""\
DataFrame()
+---------+
| max_abs |
+---------+
| 20.9    |
+---------+\
""")


def test_upstream_udaf_executes_on_distributed_workers(
    weather_ctx: DistributedSessionContext,
) -> None:
    total = udaf(
        SumAccumulator,
        [pa.float64()],
        pa.float64(),
        [pa.float64()],
        volatility="immutable",
        name="python_total",
    )
    weather_ctx.register_udaf(total)
    df = weather_ctx.sql('SELECT python_total("Rainfall") AS total FROM weather')

    assert anonymize_snapshot(df.execution_plan().display_indent()) == snapshot("""\
DistributedExec
  ProjectionExec: expr=[python_total(weather.Rainfall)@0 as total]
    AggregateExec: mode=Final, gby=[], aggr=[python_total(weather.Rainfall)]
      CoalescePartitionsExec
        [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
          AggregateExec: mode=Partial, gby=[], aggr=[python_total(weather.Rainfall)]
            DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[Rainfall], file_type=parquet
""")
    assert repr(df) == snapshot("""\
DataFrame()
+-------+
| total |
+-------+
| 522.8 |
+-------+\
""")


def test_upstream_udwf_executes_in_distributed_plan(
    weather_ctx: DistributedSessionContext,
) -> None:
    row_number = udwf(
        RowNumber,
        [pa.float64()],
        pa.uint64(),
        volatility="immutable",
        name="python_row_number",
    )
    weather_ctx.register_udwf(row_number)
    df = weather_ctx.sql(
        'SELECT "MaxTemp", '
        'python_row_number("MaxTemp") OVER ('
        'PARTITION BY "RainToday" ORDER BY "MaxTemp"'
        ') AS row_number '
        'FROM weather WHERE "RainToday" = \'Yes\' '
        'ORDER BY "MaxTemp" LIMIT 3'
    )

    assert anonymize_snapshot(df.execution_plan().display_indent()) == snapshot("""\
DistributedExec
  SortPreservingMergeExec: [MaxTemp@0 ASC NULLS LAST], fetch=3
    [Stage 2] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
      ProjectionExec: expr=[MaxTemp@0 as MaxTemp, python_row_number(weather.MaxTemp) PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@2 as row_number]
        LocalLimitExec: fetch=3
          WindowAggExec: wdw=[python_row_number(weather.MaxTemp) PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Ok(Field { name: "python_row_number(weather.MaxTemp) PARTITION BY [weather.RainToday] ORDER BY [weather.MaxTemp ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW", data_type: UInt64, nullable: true }), frame: WindowFrame { units: Range, start_bound: Preceding(Float64(NULL)), end_bound: CurrentRow, is_causal: false }]
            SortExec: expr=[MaxTemp@0 ASC NULLS LAST], preserve_partitioning=[true]
              [Stage 1] => NetworkShuffleExec: output_partitions=3, input_tasks=3
                RepartitionExec: partitioning=Hash([RainToday@1], 9), input_partitions=3
                  FilterExec: RainToday@1 = Yes
                    DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet, predicate=RainToday@19 = Yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= Yes AND Yes <= RainToday_max@1, required_guarantees=[RainToday in (Yes)]
""")
    assert repr(df) == snapshot("""\
DataFrame()
+---------+------------+
| MaxTemp | row_number |
+---------+------------+
| 8.4     | 1          |
| 9.5     | 2          |
| 10.7    | 3          |
+---------+------------+\
""")
