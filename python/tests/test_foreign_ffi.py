from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

try:
    import datafusion_distributed_ffi_test as foreign
except ImportError:
    foreign = None

pytestmark = pytest.mark.skipif(
    foreign is None,
    reason="build python/tests/ffi_test_package to run third-party FFI tests",
)

from datafusion import SessionConfig, SessionContext, udaf, udf, udwf  # noqa: E402

try:
    from datafusion_distributed import _internal
except ImportError:
    _internal = None


def _require_distributed_planner() -> None:
    if _internal is None:
        pytest.skip("datafusion-distributed extension module has not been built")
    if not hasattr(SessionContext(), "__datafusion_query_planner__"):
        pytest.skip("datafusion-python does not expose query-planner FFI")


def _context(config: SessionConfig | None = None):
    ctx = SessionContext(config) if config is not None else SessionContext()
    codec = foreign.ForeignExtensionCodec(ctx)
    ctx = ctx.with_logical_extension_codec(codec)
    ctx = ctx.with_physical_extension_codec(codec)
    ctx.register_table("foreign_table", foreign.ForeignTableProvider())
    ctx.register_udf(udf(foreign.ForeignIsNullUDF()))
    ctx.register_udaf(udaf(foreign.ForeignSumUDF()))
    ctx.register_udwf(udwf(foreign.ForeignRankUDF()))
    return ctx, codec


def test_foreign_package_executes_locally() -> None:
    ctx, _ = _context()

    grouped = ctx.sql(
        """
        SELECT category, foreign_sum(value) AS total
        FROM foreign_table
        WHERE NOT foreign_is_null(value)
        GROUP BY category
        ORDER BY category
        """
    ).collect()
    assert grouped[0].to_pydict() == {
        "category": ["a", "b"],
        "total": [10, 50],
    }

    ranked = ctx.sql(
        """
        SELECT value, foreign_rank() OVER (ORDER BY value) AS rank
        FROM foreign_table
        WHERE value IS NOT NULL
        ORDER BY value
        """
    ).collect()
    assert ranked[0].to_pydict() == {
        "value": [10, 20, 30],
        "rank": [1, 2, 3],
    }

    plan = str(ctx.sql("SELECT * FROM foreign_table").execution_plan())
    assert "ForeignScanExec" in plan


def test_foreign_package_crosses_serialized_distributed_planner() -> None:
    _require_distributed_planner()

    with TemporaryDirectory() as directory:
        path = Path(directory) / "input.csv"
        path.write_text("value,category\n10,a\n20,a\n30,b\n40,b\n")

        distributed_config = _internal.DistributedConfig()
        distributed_config.with_file_scan_config_bytes_per_partition(1)
        distributed_config.with_max_tasks_per_stage(2)
        config = SessionConfig().with_extension(distributed_config)
        ctx, codec = _context(config)
        ctx.register_csv("input", str(path))

        resolver = _internal.LocalhostChannelResolver([50051, 50052])
        ctx = _internal.with_distributed_query_planner(
            ctx, resolver, config=distributed_config
        )

        plan = str(
            ctx.sql(
                """
                SELECT category, foreign_sum(value) AS total,
                       foreign_is_null(foreign_sum(value)) AS total_is_null
                FROM input
                GROUP BY category
                """
            ).execution_plan()
        )

    assert "DistributedExec" in plan
    assert codec.physical_plan_encode_calls() > 0
    assert codec.udf_encode_calls() > 0
    assert codec.udf_decode_calls() > 0


def test_foreign_package_executes_distributed() -> None:
    _require_distributed_planner()

    distributed_config = _internal.DistributedConfig()
    distributed_config.with_max_tasks_per_stage(2)
    config = SessionConfig().with_extension(distributed_config)
    ctx, _ = _context(config)
    workers = _internal.LocalhostWorkerCluster(ctx, worker_count=2)
    ctx = _internal.with_distributed_query_planner(
        ctx, workers.resolver(), config=distributed_config
    )

    grouped = ctx.sql(
        """
        SELECT category, foreign_sum(value) AS total,
               foreign_is_null(foreign_sum(value)) AS total_is_null
        FROM foreign_table
        GROUP BY category
        ORDER BY category
        """
    ).collect()
    assert grouped[0].to_pydict() == {
        "category": ["a", "b"],
        "total": [10, 50],
        "total_is_null": [False, False],
    }

    ranked = ctx.sql(
        """
        SELECT value, foreign_rank() OVER (ORDER BY value) AS rank
        FROM foreign_table
        WHERE value IS NOT NULL
        ORDER BY value
        """
    ).collect()
    assert ranked[0].to_pydict() == {
        "value": [10, 20, 30],
        "rank": [1, 2, 3],
    }
