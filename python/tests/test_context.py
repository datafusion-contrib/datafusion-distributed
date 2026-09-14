from pathlib import Path

import pytest
from conftest import LocalhostWorkerResolver
from datafusion import DataFrame as DataFusionDataFrame
from datafusion import RuntimeEnvBuilder as DataFusionRuntimeEnvBuilder
from datafusion import SessionConfig as DataFusionSessionConfig
from datafusion import SessionContext, col, udtf
from datafusion_distributed import (
    DataFrame,
    DistributedSessionContext,
    RuntimeEnvBuilder,
    SessionConfig,
)
from datafusion_distributed.object_store import LocalFileSystem
from inline_snapshot import snapshot
from snapshot_utils import anonymize_snapshot


def test_distributed_context_requires_python_worker_resolver() -> None:
    with pytest.raises(TypeError, match="must define a get_urls"):
        DistributedSessionContext(object())  # type: ignore[arg-type]


def test_distributed_context_owns_session_and_plans_through_proto(
    localhost_worker_resolver: LocalhostWorkerResolver,
) -> None:
    ctx = DistributedSessionContext(
        localhost_worker_resolver,
        SessionConfig().with_target_partitions(2),
    )

    plan = ctx.sql(
        "SELECT n, count(*) FROM (SELECT 1 AS n UNION ALL SELECT 2 AS n) GROUP BY n"
    ).execution_plan()
    assert plan.display_indent() == snapshot("""\
DistributedExec
  CoalescePartitionsExec
    [Stage 2] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
      ProjectionExec: expr=[n@0 as n, count(Int64(1))@1 as count(*)]
        AggregateExec: mode=FinalPartitioned, gby=[n@0 as n], aggr=[count(Int64(1))], ordering_mode=Sorted
          [Stage 1] => NetworkShuffleExec: output_partitions=2, input_tasks=2
            RepartitionExec: partitioning=Hash([n@0], 4), input_partitions=1
              AggregateExec: mode=Partial, gby=[n@0 as n], aggr=[count(Int64(1))], ordering_mode=Sorted
                DistributedUnionExec: t0:[c0] t1:[c1]
                  ProjectionExec: expr=[1 as n]
                    PlaceholderRowExec
                  ProjectionExec: expr=[2 as n]
                    PlaceholderRowExec
""")


def test_mirrored_api_uses_upstream_docstrings() -> None:
    assert DataFrame.__doc__ == DataFusionDataFrame.__doc__
    assert DataFrame.__init__.__doc__ == DataFusionDataFrame.__init__.__doc__
    assert SessionConfig.__doc__ == DataFusionSessionConfig.__doc__
    assert SessionConfig.__init__.__doc__ == DataFusionSessionConfig.__init__.__doc__
    assert RuntimeEnvBuilder.__doc__ == DataFusionRuntimeEnvBuilder.__doc__
    assert (
        RuntimeEnvBuilder.__init__.__doc__
        == DataFusionRuntimeEnvBuilder.__init__.__doc__
    )
    assert DistributedSessionContext.__doc__ == SessionContext.__doc__

    for method_name in ("__getitem__", "into_view", "transform"):
        assert (
            getattr(DataFrame, method_name).__doc__
            == getattr(DataFusionDataFrame, method_name).__doc__
        )

    for method_name in (
        "enable_url_table",
        "register_udaf",
        "register_table",
        "register_udf",
        "register_udtf",
        "register_udwf",
        "register_view",
        "table_provider",
        "with_extensions",
        "with_logical_extension_codec",
        "with_physical_extension_codec",
        "with_python_udf_inlining",
    ):
        assert (
            getattr(DistributedSessionContext, method_name).__doc__
            == getattr(SessionContext, method_name).__doc__
        )


def test_udtf_registration_explains_why_it_is_unsupported(
    localhost_worker_resolver: LocalhostWorkerResolver,
) -> None:
    ctx = DistributedSessionContext(localhost_worker_resolver)

    @udtf("unsupported_table_function")
    def unsupported_table_function() -> None:
        raise AssertionError("the UDTF must not be invoked during registration")

    with pytest.raises(NotImplementedError) as error:
        ctx.register_udtf(unsupported_table_function)

    assert str(error.value) == snapshot(
        "DistributedSessionContext does not support UDTFs yet. A UDTF can "
        "materialize an arbitrary TableProvider while the logical plan is being "
        "built, and datafusion-python does not currently provide a serialization "
        "or FFI export protocol that lets this extension import a Python "
        "TableFunction safely. Supporting this requires such a TableFunction "
        "protocol together with a codec for every TableProvider it can return."
    )


def test_tables_returned_by_distributed_context_roundtrip(
    localhost_worker_resolver: LocalhostWorkerResolver,
) -> None:
    ctx = DistributedSessionContext(localhost_worker_resolver)
    source = ctx.sql("SELECT 1 AS value")

    ctx.register_table("values_view", source.into_view())
    ctx.register_table("values_copy", ctx.table_provider("values_view"))
    df = ctx.sql("SELECT value + 1 AS value FROM values_copy")

    assert anonymize_snapshot(df.execution_plan().display_indent()) == snapshot("""\
ProjectionExec: expr=[1 + 1 as value]
  PlaceholderRowExec
""")
    assert repr(df) == snapshot("""\
DataFrame()
+-------+
| value |
+-------+
| 2     |
+-------+\
""")


def test_registering_upstream_dataframe_explains_why_it_is_unsupported(
    localhost_worker_resolver: LocalhostWorkerResolver,
) -> None:
    ctx = DistributedSessionContext(localhost_worker_resolver)
    upstream_df = SessionContext().sql("SELECT 1")

    with pytest.raises(NotImplementedError) as error:
        ctx.register_table("upstream", upstream_df)

    assert str(error.value) == snapshot(
        "DistributedSessionContext cannot register a DataFrame or Table created "
        "by the separately loaded upstream datafusion native module. Use a DataFrame "
        "or Table returned by this context, a PyArrow Dataset, or an object "
        "implementing __datafusion_table_provider__. Supporting arbitrary upstream "
        "DataFrame and Table values requires datafusion-python to expose a stable FFI "
        "or serialization protocol for their native table provider or logical plan."
    )


def test_dataframe_accepts_upstream_expressions(
    weather_ctx: DistributedSessionContext,
) -> None:
    weather = weather_ctx.table("weather")
    rain_today = weather.column("RainToday")
    df = (
        weather.filter(rain_today == "Yes")
        .select(
            col('weather."MinTemp"'),
            col('weather."MaxTemp"'),
            (col('weather."MaxTemp"') - col('weather."MinTemp"')).alias(
                "temperature_range"
            ),
        )
        .sort(col("temperature_range").sort(ascending=False, nulls_first=False))
        .limit(3)
    )

    assert anonymize_snapshot(df.execution_plan().display_indent()) == snapshot("""\
DistributedExec
  SortPreservingMergeExec: [temperature_range@2 DESC NULLS LAST], fetch=3
    [Stage 1] => NetworkCoalesceExec: output_partitions=9, input_tasks=3
      SortExec: TopK(fetch=3), expr=[temperature_range@2 DESC NULLS LAST], preserve_partitioning=[true]
        ProjectionExec: expr=[MinTemp@0 as MinTemp, MaxTemp@1 as MaxTemp, MaxTemp@1 - MinTemp@0 as temperature_range]
          FilterExec: RainToday@2 = Yes, projection=[MinTemp@0, MaxTemp@1]
            DistributedLeafExec: DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, RainToday], file_type=parquet, predicate=RainToday@19 = Yes AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= Yes AND Yes <= RainToday_max@1, required_guarantees=[RainToday in (Yes)]
""")
    assert repr(df) == snapshot("""\
DataFrame()
+---------+---------+--------------------+
| MinTemp | MaxTemp | temperature_range  |
+---------+---------+--------------------+
| 11.7    | 30.0    | 18.3               |
| 9.5     | 27.4    | 17.9               |
| 17.0    | 33.8    | 16.799999999999997 |
+---------+---------+--------------------+\
""")


def test_distributed_object_store_can_be_registered(
    localhost_worker_resolver: LocalhostWorkerResolver,
) -> None:
    ctx = DistributedSessionContext(
        localhost_worker_resolver,
        SessionConfig().with_target_partitions(2),
    )
    weather_path = (
        Path(__file__).parents[2] / "testdata" / "weather" / "result-000000.parquet"
    )
    ctx.register_parquet(
        "weather_from_store",
        weather_path.as_uri(),
        object_store=LocalFileSystem(),
    )
    ctx.sql("SET distributed.file_scan_config_bytes_per_partition = 1")
    df = ctx.sql(
        "SELECT count(*) AS rows FROM weather_from_store WHERE \"RainToday\" = 'Yes'"
    )

    assert anonymize_snapshot(df.execution_plan().display_indent()) == snapshot("""\
DistributedExec
  ProjectionExec: expr=[count(Int64(1))@0 as rows]
    AggregateExec: mode=Final, gby=[], aggr=[count(Int64(1))]
      CoalescePartitionsExec
        [Stage 1] => NetworkCoalesceExec: output_partitions=6, input_tasks=3
          AggregateExec: mode=Partial, gby=[], aggr=[count(Int64(1))]
            FilterExec: RainToday@0 = Yes, projection=[]
              RepartitionExec: partitioning=RoundRobinBatch(2), input_partitions=1
                DistributedLeafExec: DataSourceExec: file_groups={1 group: [[/testdata/weather/result-000000.parquet]]}, projection=[RainToday], file_type=parquet, predicate=RainToday@19 = Yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= Yes AND Yes <= RainToday_max@1, required_guarantees=[RainToday in (Yes)]
""")
    assert repr(df) == snapshot("""\
DataFrame()
+------+
| rows |
+------+
| 33   |
+------+\
""")
