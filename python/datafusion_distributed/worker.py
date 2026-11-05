"""DataFusion Distributed worker server.

Native lifecycle events use the ``datafusion_distributed.worker`` Python logger.
The command-line entry point enables INFO output; embedded applications retain
control of their own logging configuration.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from . import _internal


class Worker:
    """A worker server running in the current Python process."""

    def __init__(self, host: str = "127.0.0.1", port: int = 50051) -> None:
        if not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        self.host = host
        self.port = port
        self._worker = _internal.Worker()
        self._url: str | None = None

    @property
    def url(self) -> str:
        """Return the URL coordinators use to reach this worker."""
        return self._url or f"http://{self.host}:{self.port}"

    def start(self) -> Worker:
        """Start serving in the background and return this worker."""
        self._url = self._worker.start(self.host, self.port)
        return self

    def stop(self) -> None:
        """Stop a worker started with :meth:`start`."""
        self._worker.stop()
        self._url = None

    def run(self) -> None:
        """Serve requests until interrupted."""
        self._worker.run(self.host, self.port)

    def __enter__(self) -> Worker:  # noqa: PYI034 - Python 3.10 has no typing.Self
        return self.start()

    def __exit__(self, *_: object) -> None:
        self.stop()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run a DataFusion Distributed worker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=50051, type=int)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )
    try:
        Worker(args.host, args.port).run()
    except KeyboardInterrupt:
        pass
