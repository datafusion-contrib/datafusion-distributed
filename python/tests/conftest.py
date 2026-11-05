"""Shared pytest fixtures for the Python test suite.

Pytest discovers ``conftest.py`` automatically, so tests request its fixtures by
using their names as function arguments and do not import them. The session-scoped
generator below starts the worker fleet once, suspends at ``yield`` while tests run,
and resumes afterward to stop every worker during session teardown.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Generator

import pytest
from datafusion_distributed import DistributedSessionContext, SessionConfig, Worker

WORKER_COUNT = 3


class LocalhostWorkerResolver:
    def __init__(self, urls: list[str]) -> None:
        self.urls = urls

    def get_urls(self) -> list[str]:
        return self.urls


@pytest.fixture(scope="session")
def localhost_worker_resolver() -> Generator[LocalhostWorkerResolver, Any, None]:
    """Run a same-process fleet of real gRPC workers."""
    workers = [Worker(port=0) for _ in range(WORKER_COUNT)]

    try:
        for worker in workers:
            worker.start()
        yield LocalhostWorkerResolver([worker.url for worker in workers])
    finally:
        for worker in reversed(workers):
            worker.stop()


@pytest.fixture(scope="session")
def weather_ctx(
    localhost_worker_resolver: LocalhostWorkerResolver,
) -> DistributedSessionContext:
    """Create a distributed context backed by the shared weather table."""
    ctx = DistributedSessionContext(
        localhost_worker_resolver,
        SessionConfig().with_target_partitions(WORKER_COUNT),
    )
    weather_path = Path(__file__).parents[2] / "testdata" / "weather"
    ctx.register_parquet("weather", weather_path)
    ctx.sql("SET distributed.file_scan_config_bytes_per_partition = 1")
    return ctx
