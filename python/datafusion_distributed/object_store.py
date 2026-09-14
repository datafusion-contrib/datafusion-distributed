"""Object stores owned by the distributed context's native extension.

These names mirror :mod:`datafusion.object_store`, but they cannot alias its
classes. ``DistributedSessionContext`` is backed by the separately loaded
``datafusion_distributed._internal`` extension, whose Rust
``register_object_store`` method extracts one of its own PyO3 storage-context
classes. An identically named instance from the installed :mod:`datafusion`
extension has a different Python type identity and cannot be extracted.

Object stores expose neither a protobuf representation nor an FFI import/export
protocol, so this module re-exports the classes compiled into ``_internal``. A
store registered on the coordinator is runtime state and is not serialized in a
query plan. Distributed workers must install equivalent stores and credentials;
the current Python ``Worker`` needs a future ``WorkerSessionBuilder`` hook before
it can configure remote stores this way.
"""

from ._internal import object_store

# Keep the same public names and constructor signatures as datafusion.object_store,
# while deliberately sourcing the class objects from the local native module.
AmazonS3 = object_store.AmazonS3
GoogleCloud = object_store.GoogleCloud
Http = object_store.Http
LocalFileSystem = object_store.LocalFileSystem
MicrosoftAzure = object_store.MicrosoftAzure

__all__ = ["AmazonS3", "GoogleCloud", "Http", "LocalFileSystem", "MicrosoftAzure"]
