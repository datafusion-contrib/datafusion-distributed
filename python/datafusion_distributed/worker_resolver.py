"""Worker discovery for distributed sessions."""

from typing import Protocol


class WorkerResolver(Protocol):
    """Resolves the workers currently available to a distributed session."""

    def get_urls(self) -> list[str]:
        """Return worker URLs such as ``http://127.0.0.1:50051``."""
        ...
