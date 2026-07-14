from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from datafusion import SessionConfig, SessionContext, col

try:
    from datafusion_distributed import _internal
except ImportError:
    _internal = None


def _physical_plan_text(ctx: SessionContext, sql: str) -> str:
    return str(ctx.sql(sql).execution_plan())


def _require_query_planner_ffi() -> None:
    if _internal is None:
        pytest.skip("datafusion-distributed extension module has not been built")

    if not hasattr(SessionContext(), "__datafusion_query_planner__") or not hasattr(
        SessionContext(), "with_query_planner"
    ):
        pytest.skip("datafusion-python does not expose the query-planner FFI API")


def test_distributed_query_planner_injects_network_shuffle_and_coalesce() -> None:
    _require_query_planner_ffi()

    with TemporaryDirectory() as directory:
        path = Path(directory) / "input.csv"
        rows = "\n".join(f"{index % 3},{index}" for index in range(100))
        path.write_text(f"k,v\n{rows}\n")

        distributed_config = _internal.DistributedConfig()
        distributed_config.with_file_scan_config_bytes_per_partition(1)
        distributed_config.with_max_tasks_per_stage(4)

        config = SessionConfig().with_extension(distributed_config)
        ctx = SessionContext(config)
        ctx.register_csv("t", str(path))

        resolver = _internal.LocalhostChannelResolver([50051, 50052, 50053, 50054])
        ctx = _internal.with_distributed_query_planner(
            ctx, resolver, config=distributed_config
        )

        plan = str(
            ctx.sql("SELECT * FROM t")
            .repartition_by_hash(col("k"), num=4)
            .aggregate([col("k")], [])
            .execution_plan()
        )

        assert "DistributedExec" in plan
        assert "NetworkShuffleExec" in plan
        assert "NetworkCoalesceExec" in plan


def test_default_query_planner_does_not_produce_distributed_physical_plan() -> None:
    ctx = SessionContext()

    plan = _physical_plan_text(ctx, "SELECT 1 AS value")

    assert "DistributedExec" not in plan
