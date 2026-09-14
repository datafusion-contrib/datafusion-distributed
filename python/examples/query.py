"""Run SQL against two localhost workers."""

import argparse
from pathlib import Path

from datafusion_distributed import DistributedSessionContext, WorkerResolver


class Workers(WorkerResolver):
    def __init__(self, ports: list[int]) -> None:
        self.ports = ports

    def get_urls(self) -> list[str]:
        # Replace this with service discovery in a real deployment.
        return [f"http://127.0.0.1:{port}" for port in self.ports]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", action="append", dest="ports", type=int)
    parser.add_argument("sql", help="SQL statement to execute")
    args = parser.parse_args()

    ctx = DistributedSessionContext(Workers(args.ports or [50051, 50052]))
    # Register a sample `weather` table.
    ctx.register_parquet("weather", Path(__file__).parents[2] / "testdata" / "weather")
    # Force the distributed planner to distribute very aggressively.
    ctx.sql("SET distributed.file_scan_config_bytes_per_partition = 1")
    ctx.sql(args.sql).show()
