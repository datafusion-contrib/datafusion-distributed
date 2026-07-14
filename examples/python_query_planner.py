"""Run a DataFusion Python query through datafusion-distributed's query planner.

This example assumes one or more datafusion-distributed workers are already
listening on localhost. For example, start workers from this repository with
ports matching WORKER_PORTS before running this script.
"""

from __future__ import annotations

import os

from datafusion import SessionConfig, SessionContext
import datafusion_distributed as dd


def worker_ports() -> list[int]:
    ports = os.environ.get("WORKER_PORTS", "50051")
    return [int(port) for port in ports.split(",") if port]


config = SessionConfig().with_extension(dd.DistributedConfig())
ctx = SessionContext(config)

resolver = dd.LocalhostChannelResolver(worker_ports())
ctx = dd.with_distributed_query_planner(ctx, resolver)

ctx.sql("SELECT 1 AS value").show()
