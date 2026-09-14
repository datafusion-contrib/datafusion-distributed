"""Public Python bindings for DataFusion Distributed.

Configuration classes are exposed here because their opaque native values must
be created by the same extension as :class:`DistributedSessionContext`. Values
with protobuf or FFI bridges continue to use their normal :mod:`datafusion`
public classes; see the package README and the compatibility modules for details.
"""

from .context import (
    DistributedSessionContext,
    RuntimeEnvBuilder,
    SessionConfig,
)
from .dataframe import DataFrame
from .worker import Worker
from .worker_resolver import WorkerResolver

__all__ = [
    "DataFrame",
    "DistributedSessionContext",
    "RuntimeEnvBuilder",
    "SessionConfig",
    "Worker",
    "WorkerResolver",
]
