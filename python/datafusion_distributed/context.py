"""Distributed session context and local configuration wrappers.

The installed :mod:`datafusion` package and this package's ``_internal`` module
are different native extensions. Although both compile the same datafusion-python
Rust classes, PyO3 gives each extension's classes a distinct Python type identity.

``DistributedSessionContext`` owns the session created by ``_internal``. Its
``SessionConfig`` and ``RuntimeEnvBuilder`` must therefore allocate their opaque
native values in that module; upstream instances cannot be downcast by the local
session and datafusion-python exposes no import/export protocol for them. The
wrappers below deliberately retain the upstream Python API and docstrings while
replacing only construction of the native value.

Values with a stable bridge do not need parallel public classes. Expressions and
supported UDFs cross through protobuf in :mod:`.dataframe` and the registration
methods below, while custom table providers can use DataFusion's FFI protocol.
"""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from datafusion import DataFrame as DataFusionDataFrame
from datafusion import RuntimeEnvBuilder as DataFusionRuntimeEnvBuilder
from datafusion import SessionConfig as DataFusionSessionConfig
from datafusion import SessionContext
from datafusion.catalog import Table
from datafusion.context import TableProviderExportable

from . import _internal
from .dataframe import DataFrame
from .worker_resolver import WorkerResolver

if TYPE_CHECKING:
    from datafusion.user_defined import (
        AggregateUDF,
        ScalarUDF,
        TableFunction,
        WindowUDF,
    )


class SessionConfig(DataFusionSessionConfig):
    """Session configuration options."""

    def __init__(self, config_options: dict[str, str] | None = None) -> None:
        """Create a new :py:class:`SessionConfig` with the given configuration options.

        Args:
            config_options: Configuration options.
        """
        # Calling the upstream constructor would allocate datafusion._internal's
        # distinct PySessionConfig, which the local PySessionContext cannot extract.
        self.config_internal = _internal.SessionConfig(config_options)


class RuntimeEnvBuilder(DataFusionRuntimeEnvBuilder):
    """Runtime configuration options."""

    def __init__(self) -> None:
        """Create a new :py:class:`RuntimeEnvBuilder` with default values."""
        # See SessionConfig: this opaque builder must share the context's module.
        self.config_internal = _internal.RuntimeEnvBuilder()


class DistributedSessionContext(SessionContext):
    """This is the main interface for executing queries and creating DataFrames.

    See :ref:`user_guide_concepts` in the online documentation for more information.

    **A context is a handle on a session, not the session itself.** The
    ``with_*`` methods — :py:meth:`with_logical_extension_codec`,
    :py:meth:`with_physical_extension_codec`,
    :py:meth:`with_python_udf_inlining`, and :py:meth:`with_extensions` —
    return a new context wrapping the *same* underlying session. Only the
    Python-side codec settings differ; catalogs, tables, registered functions,
    and configuration are the one shared session, so a registration through
    either handle is visible to both.

    A few things therefore belong to the session rather than to a handle, and
    take effect even if the handle that set them is discarded: the query
    planner (see :py:meth:`set_query_planner`), and the rebuild of an installed
    foreign planner that follows installing a codec. :ref:`extension_sessions`
    in the online documentation works through when that matters.

    **Keep a context alive for as long as anything derived from it is in use.**
    A :py:class:`~datafusion.DataFrame`, logical plan, or exported capsule does
    not extend the session's lifetime. Once the last context on a session is
    collected, any operation that reaches an extension codec fails with
    ``TaskContextProvider went out of scope over FFI boundary``.
    """

    def __init__(
        self,
        worker_resolver: WorkerResolver,
        config: SessionConfig | None = None,
        runtime: RuntimeEnvBuilder | None = None,
    ) -> None:
        """Main interface for executing queries with DataFusion.

        Maintains the state of the connection between a user and an instance
        of the connection between a user and an instance of the DataFusion
        engine.

        Args:
            worker_resolver: Python worker-discovery implementation.
            config: Session configuration options.
            runtime: Runtime configuration options.

        Example usage:

        The following example demonstrates how to use the context to execute
        a query against a CSV data source using the :py:class:`DataFrame` API::

            from datafusion_distributed import DistributedSessionContext

            ctx = DistributedSessionContext(worker_resolver)
            df = ctx.read_csv("data.csv")
        """
        config = config.config_internal if config is not None else None
        runtime = runtime.config_internal if runtime is not None else None

        self.ctx = _internal.create_distributed_session(
            worker_resolver, config, runtime
        )

    def __getattribute__(self, name: str) -> Any:
        attribute = super().__getattribute__(name)
        if name.startswith("_") or not callable(attribute):
            return attribute

        @wraps(attribute)
        def invoke(*args: Any, **kwargs: Any) -> Any:
            result = attribute(*args, **kwargs)
            if isinstance(result, DataFrame):
                return result
            if isinstance(result, DataFusionDataFrame):
                return DataFrame._from_context(result.df, self)
            return result

        return invoke

    @classmethod
    def _wrap(cls, context: SessionContext) -> DistributedSessionContext:
        wrapped = cls.__new__(cls)
        wrapped.ctx = context.ctx
        _internal.rebind_distributed_planner(wrapped.ctx)
        return wrapped

    def enable_url_table(self) -> DistributedSessionContext:
        """Control if local files can be queried as tables.

        Returns:
            A new :py:class:`SessionContext` object with url table enabled.
        """
        return self._wrap(super().enable_url_table())

    def register_udf(self, udf: ScalarUDF) -> None:
        """Register a user-defined function (UDF) with the context."""
        _internal.register_scalar_udf(self.ctx, udf().to_bytes())

    def register_udaf(self, udaf: AggregateUDF) -> None:
        """Register a user-defined aggregation function (UDAF) with the context."""
        _internal.register_aggregate_udf(self.ctx, udaf().to_bytes())

    def register_udwf(self, udwf: WindowUDF) -> None:
        """Register a user-defined window function (UDWF) with the context."""
        _internal.register_window_udf(self.ctx, udwf().to_bytes())

    def register_udtf(self, func: TableFunction) -> None:
        """Register a user defined table function."""
        raise NotImplementedError(
            "DistributedSessionContext does not support UDTFs yet. A UDTF can "
            "materialize an arbitrary TableProvider while the logical plan is being "
            "built, and datafusion-python does not currently provide a serialization "
            "or FFI export protocol that lets this extension import a Python "
            "TableFunction safely. Supporting this requires such a TableFunction "
            "protocol together with a codec for every TableProvider it can return."
        )

    def register_view(self, name: str, df: DataFrame) -> None:
        """Register a :py:class:`~datafusion.dataframe.DataFrame` as a view.

        Args:
            name (str): The name to register the view under.
            df (DataFrame): The DataFrame to be converted into a view and registered.
        """
        self.register_table(name, df)

    def register_table(
        self,
        name: str,
        table: Table | TableProviderExportable | DataFrame | pa.dataset.Dataset,
    ) -> None:
        """Register a :py:class:`~datafusion.Table` with this context.

        The registered table can be referenced from SQL statements executed against
        this context.

        Args:
            name: Name of the resultant table.
            table: Any object that can be converted into a :class:`Table`.
        """
        try:
            self.ctx.register_table(name, table)
        except (TypeError, ValueError) as error:
            if isinstance(table, (DataFusionDataFrame, Table)):
                raise NotImplementedError(
                    "DistributedSessionContext cannot register a DataFrame or Table "
                    "created by the separately loaded upstream datafusion native "
                    "module. Use a DataFrame or Table returned by this context, a "
                    "PyArrow Dataset, or an object implementing "
                    "__datafusion_table_provider__. Supporting arbitrary upstream "
                    "DataFrame and Table values requires datafusion-python to expose "
                    "a stable FFI or serialization protocol for their native table "
                    "provider or logical plan."
                ) from error
            raise

    def table_provider(self, name: str) -> Table:
        """Return the :py:class:`~datafusion.catalog.Table` for the given table name.

        Args:
            name: Name of the table.

        Returns:
            The table provider.

        Raises:
            KeyError: If the table is not found.

        Examples:
            >>> import pyarrow as pa
            >>> ctx = SessionContext()
            >>> batch = pa.RecordBatch.from_pydict({"x": [1, 2]})
            >>> ctx.register_record_batches("my_table", [[batch]])
            >>> tbl = ctx.table_provider("my_table")
            >>> tbl.schema
            x: int64
        """
        # Table(raw) would invoke the other extension's RawTable constructor,
        # which cannot recognize this already-valid local RawTable instance.
        wrapped = Table.__new__(Table)
        wrapped._inner = self.ctx.table_provider(name)
        return wrapped

    def with_extensions(self, *extensions: Any) -> DistributedSessionContext:
        """Create a new session context with the given extension bundles.

        This is the preferred way to install extension codecs and query
        planners, because it removes the ordering question that installing them
        by hand creates.

        Each argument is called twice, in two phases:

        1. ``__datafusion_session_components__(ctx)`` on every extension, then
           all the returned codecs are installed at once.
        2. ``__datafusion_session_planner__(ctx, fallback)`` on every
           extension, **in argument order**, each handed the planner built so
           far and the context carrying every bundle's codecs. The last
           extension listed ends up outermost.

        An extension implements either hook or both. Return ``None`` from the
        planner hook to contribute no planner; see
        :py:class:`~datafusion.extensions.SessionPlannerExportable`.

        Nothing is written to the session until every hook has returned and
        every capsule has been validated, so a hook that raises leaves the
        session as it was. A hook that *mutates* the context it is handed —
        registering a table, say — is not rolled back, which is why bundle
        objects must be configuration-only.

        Shares its session with this context — see :py:class:`SessionContext`.

        See :ref:`extension_bundles` in the online documentation for why the
        phases are split, how to contribute a bundle's two halves at different
        positions, and a worked Rust implementation.

        Args:
            extensions: Extension bundles to install. Order is irrelevant for
                codecs and significant for planners, which nest in this order
                with the last one outermost. Passing none installs nothing and
                returns a handle on this session, so a caller assembling the
                list at runtime need not special-case it being empty.

        Returns:
            A new context with all extension components installed.

        Raises:
            TypeError: If an argument implements neither hook, if a hook
                returns the wrong type, or if a codec is contributed as a bare
                ``PyCapsule`` rather than an object exposing the getter.
            ValueError: If two codecs claim the same id, or a getter returns a
                capsule of the wrong kind. See
                :py:meth:`with_logical_extension_codec` for how ids are
                assigned.

        Examples:
            The returned handle is a different object sharing one session, and
            an empty call is legal:

            >>> from datafusion import SessionContext
            >>> ctx = SessionContext()
            >>> derived = ctx.with_extensions()
            >>> derived is ctx
            False
            >>> ctx.from_pydict({"a": [1, 2]}, name="t")  # doctest: +ELLIPSIS
            DataFrame()...
            >>> derived.table_exist("t")
            True
            >>> derived.logical_extension_codec_ids()
            []

            A runnable multi-bundle example, showing what a bundle returns and
            how its codec ids accumulate, is in :ref:`extension_bundles`.

            Real usage. Skipped here (needs a built extension library); run
            verbatim by ``test_with_extensions_docstring_example_still_runs``.

            >>> from my_extension import DistributedEngineExtension  # doctest: +SKIP
            >>> ctx = SessionContext().with_extensions(
            ...     DistributedEngineExtension("scheduler:50050")
            ... )  # doctest: +SKIP
            >>> batches = ctx.sql("SELECT 1 AS n").collect()  # doctest: +SKIP
            >>> batches[0].column(0).to_pylist()  # doctest: +SKIP
            [1]
        """
        return self._wrap(super().with_extensions(*extensions))

    def with_logical_extension_codec(
        self, codec: Any, codec_id: str | None = None
    ) -> DistributedSessionContext:
        """Create a new session context with an additional logical codec.

        Only FFI codecs are supported. Pass any object implementing
        ``__datafusion_logical_extension_codec__`` (see
        :py:class:`~datafusion.user_defined.LogicalExtensionCodecExportable`).

        Codecs compose: each call appends the codec rather than replacing
        codecs installed earlier, so one session can carry codecs from several
        independent libraries and the order they are installed in does not
        affect decoding.

        A serialized plan records which codec wrote each payload, as a short id
        taken from the codec's class.

        Shares its session with this context — see :py:class:`SessionContext`.

        See :ref:`extension_codec_ids` in the online documentation for how ids
        are assigned and what an extension codec has to implement, and
        :py:meth:`with_extensions` for installing a library's codecs and planner
        together.

        Args:
            codec: Object implementing ``__datafusion_logical_extension_codec__``
                (see
                :py:class:`~datafusion.user_defined.LogicalExtensionCodecExportable`),
                or a raw ``datafusion_logical_extension_codec`` PyCapsule.
            codec_id: Overrides the id the codec's payloads are tagged with.
                Normally unnecessary. Pass it when installing two instances of
                one class, which otherwise claim the same id, and when
                installing a bare ``PyCapsule``: a capsule has no class to take
                an id from, so it is given a random ``anon:`` id that differs on
                every install, and plans it encodes can never be decoded
                elsewhere.

        Returns:
            A new context carrying this codec in addition to any already
            installed.

        Raises:
            ValueError: If the resolved id is already installed on this session.

        Examples:
            A context exports its own codec, which stands in here for a real
            library's:

            >>> from datafusion import SessionContext
            >>> host = SessionContext()
            >>> capsule = host.__datafusion_logical_extension_codec__()
            >>> ctx = SessionContext().with_logical_extension_codec(
            ...     capsule, codec_id="my_library.Codec"
            ... )
            >>> ctx.logical_extension_codec_ids()
            ['my_library.Codec']

            Without ``codec_id`` a bare capsule gets an anonymous id, which is
            fine only if its plans never leave this session:

            >>> ctx = SessionContext().with_logical_extension_codec(capsule)
            >>> ctx.logical_extension_codec_ids()[0].startswith("anon:")
            True
        """
        return self._wrap(super().with_logical_extension_codec(codec, codec_id))

    def with_physical_extension_codec(
        self, codec: Any, codec_id: str | None = None
    ) -> DistributedSessionContext:
        """Create a new session context with an additional physical codec.

        Only FFI codecs are supported. Pass any object implementing
        ``__datafusion_physical_extension_codec__`` (see
        :py:class:`~datafusion.user_defined.PhysicalExtensionCodecExportable`).

        Composes and assigns an id exactly as
        :py:meth:`with_logical_extension_codec` does, including when to pass
        ``codec_id`` and what the returned context shares. See that method.

        Args:
            codec: As :py:meth:`with_logical_extension_codec`, for the physical
                getter.
            codec_id: As :py:meth:`with_logical_extension_codec`.

        Returns:
            A new context carrying this codec in addition to any already
            installed.

        Raises:
            ValueError: If the resolved id is already installed on this session.

        Examples:
            >>> from datafusion import SessionContext
            >>> host = SessionContext()
            >>> capsule = host.__datafusion_physical_extension_codec__()
            >>> ctx = SessionContext().with_physical_extension_codec(
            ...     capsule, codec_id="my_library.PhysicalCodec"
            ... )
            >>> ctx.physical_extension_codec_ids()
            ['my_library.PhysicalCodec']
        """
        return self._wrap(super().with_physical_extension_codec(codec, codec_id))

    def with_python_udf_inlining(self, *, enabled: bool) -> DistributedSessionContext:
        """Control whether Python UDFs are embedded in serialized expressions.

        With ``enabled=True``, serialized expressions carry the Python
        code for any scalar, aggregate, or window UDFs they reference.
        The receiver rebuilds the UDFs from those bytes and does not
        need to register them first.

        With ``enabled=False``, serialized expressions store only the
        UDF names. This has two uses:

        * **Cross-language portability.** The bytes can be decoded by a
          non-Python receiver, which must already have UDFs registered
          under matching names.
        * **Safer deserialization.** :meth:`Expr.from_bytes` will refuse
          to rebuild Python UDFs rather than call ``cloudpickle.loads``
          on untrusted input.

        The setting affects :meth:`Expr.to_bytes` and
        :meth:`Expr.from_bytes` whenever this session is passed as the
        ``ctx`` argument. :func:`pickle.dumps` and :func:`pickle.loads`
        do not pass a context, so to apply the setting through pickle,
        register this session with
        :func:`datafusion.ipc.set_sender_ctx` on the sender and
        :func:`datafusion.ipc.set_worker_ctx` on the receiver.

        .. warning:: Security
            This setting narrows only :meth:`Expr.from_bytes`. Calling
            :func:`pickle.loads` on untrusted bytes remains unsafe
            regardless of the toggle.

        Shares its session with this context — see
        :py:class:`SessionContext`. The original context's own codec
        settings are unchanged.

        Args:
            enabled: Whether to embed Python UDFs in serialized
                expressions. Keyword-only and required, so callers must
                pick a mode explicitly. Fresh sessions behave as
                ``enabled=True`` until this method overrides the toggle.

        Returns:
            A new :class:`SessionContext` with the toggle applied.

        Examples:
            >>> import pyarrow as pa
            >>> from datafusion import SessionContext, Expr, col, udf
            >>> ctx = SessionContext()
            >>> identity = udf(lambda a: a, [pa.int64()], pa.int64(),
            ...                volatility="immutable", name="identity_demo")
            >>> ctx.register_udf(identity)
            >>> blob = identity(col("x")).to_bytes(ctx)
            >>> strict = SessionContext().with_python_udf_inlining(enabled=False)
            >>> try:
            ...     Expr.from_bytes(blob, strict)
            ... except Exception as e:
            ...     print("Refusing to deserialize" in str(e))
            True
        """
        return self._wrap(super().with_python_udf_inlining(enabled=enabled))
