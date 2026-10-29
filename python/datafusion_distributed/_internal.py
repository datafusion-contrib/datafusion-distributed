"""Compatibility facade for the package's native extension.

The extension has a distinct native name because it links datafusion-python,
whose Rust crate already exports a module named ``_internal``. Re-exporting its
symbols here preserves the package's private import path and matching type stub.
"""

from ._native import (
    RuntimeEnvBuilder,
    SessionConfig,
    Worker,
    create_distributed_session,
    deserialize_expression,
    object_store,
    rebind_distributed_planner,
    register_aggregate_udf,
    register_scalar_udf,
    register_window_udf,
    serialize_expression,
)

__all__ = [
    "RuntimeEnvBuilder",
    "SessionConfig",
    "Worker",
    "create_distributed_session",
    "deserialize_expression",
    "object_store",
    "rebind_distributed_planner",
    "register_aggregate_udf",
    "register_scalar_udf",
    "register_window_udf",
    "serialize_expression",
]
