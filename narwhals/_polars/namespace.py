from __future__ import annotations

from functools import reduce
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Iterable

from narwhals import dtypes
from narwhals._expression_parsing import parse_into_exprs
from narwhals._polars.dataframe import PolarsDataFrame
from narwhals._polars.expr import PolarsExpr
# from narwhals._polars.selectors import PolarsSelectorNamespace
from narwhals._polars.series import PolarsSeries
from narwhals.utils import flatten

if TYPE_CHECKING:
    from narwhals._polars.typing import IntoPolarsExpr
    from narwhals._polars.utils import Implementation


class PolarsNamespace:
    Int64 = dtypes.Int64
    Int32 = dtypes.Int32
    Int16 = dtypes.Int16
    Int8 = dtypes.Int8
    UInt64 = dtypes.UInt64
    UInt32 = dtypes.UInt32
    UInt16 = dtypes.UInt16
    UInt8 = dtypes.UInt8
    Float64 = dtypes.Float64
    Float32 = dtypes.Float32
    Boolean = dtypes.Boolean
    Object = dtypes.Object
    Unknown = dtypes.Unknown
    Categorical = dtypes.Categorical
    Enum = dtypes.Enum
    String = dtypes.String
    Datetime = dtypes.Datetime
    Duration = dtypes.Duration
    Date = dtypes.Date

    @property
    def selectors(self) -> PolarsSelectorNamespace:
        return PolarsSelectorNamespace(
            implementation=self._implementation, backend_version=self._backend_version
        )

    # --- not in spec ---
    def __init__(
        self, backend_version: tuple[int, ...]
    ) -> None:
        self._backend_version = backend_version

    def _create_expr_from_callable(  # noqa: PLR0913
        self,
        func: Callable[[PolarsDataFrame], list[PolarsSeries]],
        *,
        depth: int,
        function_name: str,
        root_names: list[str] | None,
        output_names: list[str] | None,
    ) -> PolarsExpr:
        return PolarsExpr(
            func,
            depth=depth,
            function_name=function_name,
            root_names=root_names,
            output_names=output_names,
            backend_version=self._backend_version,
        )

    def _create_series_from_scalar(
        self, value: Any, series: PolarsSeries
    ) -> PolarsSeries:
        return PolarsSeries._from_iterable(
            [value],
            name=series._native_series.name,
            index=series._native_series.index[0:1],
            backend_version=self._backend_version,
        )

    def _create_expr_from_series(self, series: PolarsSeries) -> PolarsExpr:
        return PolarsExpr(
            lambda _df: [series],
            depth=0,
            function_name="series",
            root_names=None,
            output_names=None,
            backend_version=self._backend_version,
        )

    def _create_native_series(self, value: Any) -> Any:
        return create_native_series(
            value,
            backend_version=self._backend_version,
        )

    # --- selection ---
    def col(self, *column_names: str | Iterable[str]) -> PolarsExpr:
        return PolarsExpr(*column_names, backend_version=self._backend_version)

    def all(self) -> PolarsExpr:
        return PolarsExpr(
            lambda df: [
                PolarsSeries(
                    df._native_dataframe.loc[:, column_name],
                    backend_version=self._backend_version,
                )
                for column_name in df.columns
            ],
            depth=0,
            function_name="all",
            root_names=None,
            output_names=None,
            backend_version=self._backend_version,
        )

    def lit(self, value: Any, dtype: dtypes.DType | None) -> PolarsExpr:
        def _lit_pandas_series(df: PolarsDataFrame) -> PolarsSeries:
            pandas_series = PolarsSeries._from_iterable(
                data=[value],
                name="lit",
                index=df._native_dataframe.index[0:1],
                backend_version=self._backend_version,
            )
            if dtype:
                return pandas_series.cast(dtype)
            return pandas_series

        return PolarsExpr(
            lambda df: [_lit_pandas_series(df)],
            depth=0,
            function_name="lit",
            root_names=None,
            output_names=["lit"],
            backend_version=self._backend_version,
        )

    # --- reduction ---
    def sum(self, *column_names: str) -> PolarsExpr:
        return PolarsExpr.from_column_names(
            *column_names,
            implementation=self._implementation,
            backend_version=self._backend_version,
        ).sum()

    def mean(self, *column_names: str) -> PolarsExpr:
        return PolarsExpr.from_column_names(
            *column_names,
            implementation=self._implementation,
            backend_version=self._backend_version,
        ).mean()

    def max(self, *column_names: str) -> PolarsExpr:
        return PolarsExpr.from_column_names(
            *column_names,
            implementation=self._implementation,
            backend_version=self._backend_version,
        ).max()

    def min(self, *column_names: str) -> PolarsExpr:
        return PolarsExpr.from_column_names(
            *column_names,
            implementation=self._implementation,
            backend_version=self._backend_version,
        ).min()

    def len(self) -> PolarsExpr:
        return PolarsExpr(
            lambda df: [
                PolarsSeries._from_iterable(
                    [len(df._native_dataframe)],
                    name="len",
                    index=[0],
                    implementation=self._implementation,
                    backend_version=self._backend_version,
                )
            ],
            depth=0,
            function_name="len",
            root_names=None,
            output_names=["len"],
            implementation=self._implementation,
            backend_version=self._backend_version,
        )

    # --- horizontal ---
    def sum_horizontal(self, *exprs: IntoPolarsExpr) -> PolarsExpr:
        return reduce(
            lambda x, y: x + y,
            parse_into_exprs(
                *exprs,
                namespace=self,
            ),
        )

    def all_horizontal(self, *exprs: IntoPolarsExpr) -> PolarsExpr:
        # Why is this showing up as uncovered? It defo is?
        return reduce(
            lambda x, y: x & y,
            parse_into_exprs(*exprs, namespace=self),
        )  # pragma: no cover

    def concat(
        self,
        items: Iterable[PolarsDataFrame],
        *,
        how: str = "vertical",
    ) -> PolarsDataFrame:
        dfs: list[Any] = [item._native_dataframe for item in items]
        if how == "horizontal":
            return PolarsDataFrame(
                horizontal_concat(
                    dfs,
                    implementation=self._implementation,
                    backend_version=self._backend_version,
                ),
                implementation=self._implementation,
                backend_version=self._backend_version,
            )
        if how == "vertical":
            return PolarsDataFrame(
                vertical_concat(
                    dfs,
                    implementation=self._implementation,
                    backend_version=self._backend_version,
                ),
                implementation=self._implementation,
                backend_version=self._backend_version,
            )
        raise NotImplementedError
