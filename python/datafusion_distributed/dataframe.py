"""DataFrame compatibility across the two native datafusion-python modules.

A DataFrame returned by :class:`DistributedSessionContext` contains a native
``PyDataFrame`` owned by this package's ``_internal`` extension. The normal
:mod:`datafusion` Python wrapper can hold that object, but its methods otherwise
pass native ``Expr`` and ``SortExpr`` instances created by the separately loaded
upstream extension. PyO3 cannot downcast those instances to the local classes.

This module's ``DataFrame`` retains the upstream API while converting expression
arguments through DataFusion's protobuf format before invoking the local frame.
Expression results are converted in the opposite direction so callers continue
to work with normal :mod:`datafusion` expressions. Tables need no parallel public
class: :meth:`into_view` places the valid local table handle directly inside the
upstream Python ``Table`` wrapper instead of asking its native constructor to
reinterpret that handle.

This adapter can go away once datafusion-python provides stable cross-extension
import/export protocols for DataFrames, expressions, and tables.
"""

from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING, Any

from datafusion import DataFrame as DataFusionDataFrame
from datafusion.catalog import Table
from datafusion.expr import Expr, SortExpr

from . import _internal

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion._internal import DataFrame as DataFrameInternal


class DataFrame(DataFusionDataFrame):
    """Two dimensional table representation of data.

    DataFrame objects are iterable; iterating over a DataFrame yields
    :class:`datafusion.RecordBatch` instances lazily.

    See :ref:`user_guide_concepts` in the online documentation for more information.
    """

    def __init__(self, df: DataFrameInternal) -> None:
        """This constructor is not to be used by the end user.

        See :py:class:`~datafusion.context.SessionContext` for methods to
        create a :py:class:`DataFrame`.
        """
        super().__init__(df)

    @classmethod
    def _from_context(cls, df: DataFrameInternal, context: Any) -> DataFrame:
        wrapped = cls(df)
        wrapped._distributed_context = context
        return wrapped

    def _to_local_expression(self, expression: Expr) -> Expr:
        raw = _internal.deserialize_expression(
            self._distributed_context.ctx, expression.to_bytes()
        )
        return Expr(raw)

    def _to_local_sort_expression(self, expression: SortExpr) -> SortExpr:
        local_expression = self._to_local_expression(expression.expr())
        wrapped = SortExpr.__new__(SortExpr)
        wrapped.raw_sort = local_expression.expr.sort(
            expression.ascending(), expression.nulls_first()
        )
        return wrapped

    def _adapt_argument(self, value: Any) -> Any:
        if isinstance(value, SortExpr):
            return self._to_local_sort_expression(value)
        if isinstance(value, Expr):
            return self._to_local_expression(value)
        if isinstance(value, list):
            return [self._adapt_argument(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._adapt_argument(item) for item in value)
        if isinstance(value, dict):
            return {key: self._adapt_argument(item) for key, item in value.items()}
        return value

    def _to_upstream_expression(self, expression: Expr) -> Expr:
        encoded = _internal.serialize_expression(
            self._distributed_context.ctx, expression.expr
        )
        return Expr.from_bytes(encoded)

    def _adapt_result(self, value: Any) -> Any:
        if isinstance(value, DataFrame):
            return value
        if isinstance(value, DataFusionDataFrame):
            return DataFrame._from_context(value.df, self._distributed_context)
        if isinstance(value, Expr):
            return self._to_upstream_expression(value)
        if isinstance(value, list):
            return [self._adapt_result(item) for item in value]
        if isinstance(value, tuple):
            return tuple(self._adapt_result(item) for item in value)
        if isinstance(value, dict):
            return {key: self._adapt_result(item) for key, item in value.items()}
        return value

    def __getattribute__(self, name: str) -> Any:
        attribute = super().__getattribute__(name)
        if (
            name.startswith("_")
            or name in type(self).__dict__
            or not callable(attribute)
        ):
            return attribute

        @wraps(attribute)
        def invoke(*args: Any, **kwargs: Any) -> Any:
            raw = super(DataFrame, self).__getattribute__("df")
            base_method = getattr(DataFusionDataFrame(raw), name)
            adapt = super(DataFrame, self).__getattribute__("_adapt_argument")
            adapt_result = super(DataFrame, self).__getattribute__("_adapt_result")
            adapted_args = tuple(adapt(arg) for arg in args)
            adapted_kwargs = {key: adapt(value) for key, value in kwargs.items()}
            return adapt_result(base_method(*adapted_args, **adapted_kwargs))

        return invoke

    def __getitem__(self, key: str | list[str]) -> DataFrame:
        """Return a new :py:class:`DataFrame` with the specified column or columns.

        Args:
            key: Column name or list of column names to select.

        Returns:
            DataFrame with the specified column or columns.
        """
        result = DataFusionDataFrame(self.df).__getitem__(key)
        return DataFrame._from_context(result.df, self._distributed_context)

    def into_view(self, temporary: bool = False) -> Table:
        """Convert ``DataFrame`` into a :class:`~datafusion.Table`.

        Examples:
            >>> from datafusion import SessionContext
            >>> ctx = SessionContext()
            >>> df = ctx.sql("SELECT 1 AS value")
            >>> view = df.into_view()
            >>> ctx.register_table("values_view", view)
            >>> result = ctx.sql("SELECT value FROM values_view").collect()
            >>> result[0].column("value").to_pylist()
            [1]
        """
        # Bypass the upstream native constructor: it cannot downcast the local
        # RawTable returned by this extension, but its Python wrapper can hold it.
        wrapped = Table.__new__(Table)
        wrapped._inner = self.df.into_view(temporary)
        return wrapped

    def transform(self, func: Callable[..., DataFrame], *args: Any) -> DataFrame:
        """Apply a function to the current DataFrame which returns another DataFrame.

        This is useful for chaining together multiple functions.

        Examples:
            >>> ctx = dfn.SessionContext()
            >>> df = ctx.from_pydict({"a": [1, 2, 3]})
            >>> def add_3(df):
            ...     return df.with_column("modified", dfn.lit(3))
            >>> def within_limit(df: DataFrame, limit: int) -> DataFrame:
            ...     return df.filter(col("a") < lit(limit)).distinct()
            >>> df.transform(add_3).transform(within_limit, 4).sort("a").to_pydict()
            {'a': [1, 2, 3], 'modified': [3, 3, 3]}

        Args:
            func: A callable function that takes a DataFrame as it's first argument
            args: Zero or more arguments to pass to `func`

        Returns:
            DataFrame: After applying func to the original dataframe.
        """
        return self._adapt_result(func(self, *args))
