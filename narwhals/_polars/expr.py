from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Literal

from narwhals._expression_parsing import reuse_series_implementation
from narwhals._expression_parsing import reuse_series_namespace_implementation
from narwhals._polars.series import PolarsSeries
from narwhals._polars.utils import extract_native
from narwhals.dependencies import get_polars

if TYPE_CHECKING:
    from typing_extensions import Self

    from narwhals._polars.dataframe import PolarsDataFrame
    from narwhals._polars.namespace import PolarsNamespace
    from narwhals._polars.utils import Implementation
    import polars as pl


class PolarsExpr:
    def __init__(  # noqa: PLR0913
        self,
        expr: pl.Expr,
        backend_version: tuple[int, ...],
    ) -> None:
        self._expr = get_polars().col(expr) if isinstance(expr, str) else expr
        self._backend_version = backend_version

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"PolarsExpr("
            f"expr: {self._expr}"
        )

    def __narwhals_namespace__(self) -> PolarsNamespace:
        from narwhals._polars.namespace import PolarsNamespace

        return PolarsNamespace(self._backend_version)

    def __narwhals_expr__(self) -> None: ...

    def _from_native_expr(self, expr):
        return self.__class__(expr, backend_version=self._backend_version)

    def cast(
        self,
        dtype: Any,
    ) -> Self:
        return reuse_series_implementation(self, "cast", dtype=dtype)

    def __eq__(self, other: PolarsExpr | Any) -> Self:  # type: ignore[override]
        return self._from_native_expr(self._expr.__eq__(extract_native(other)))

    def __ne__(self, other: PolarsExpr | Any) -> Self:  # type: ignore[override]
        return self._from_native_expr(self._expr.__ne__(extract_native(other)))

    def __ge__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__ge__(extract_native(other)))

    def __gt__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__gt__(extract_native(other)))

    def __le__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__le__(extract_native(other)))

    def __lt__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__lt__(extract_native(other)))

    def __and__(self, other: PolarsExpr | bool | Any) -> Self:
        return self._from_native_expr(self._expr.__and__(extract_native(other)))

    def __rand__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__rand__(extract_native(other)))

    def __or__(self, other: PolarsExpr | bool | Any) -> Self:
        return self._from_native_expr(self._expr.__or__(extract_native(other)))

    def __ror__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__ror__(extract_native(other)))

    def __add__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__add__(extract_native(other)))

    def __radd__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__radd__(extract_native(other)))

    def __sub__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__sub__(extract_native(other)))

    def __rsub__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__rsub__(extract_native(other)))

    def __mul__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__mul__(extract_native(other)))

    def __rmul__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__rmul__(extract_native(other)))

    def __truediv__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__truediv__(extract_native(other)))

    def __rtruediv__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__rtruediv__(extract_native(other)))

    def __floordiv__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__floordiv__(extract_native(other)))

    def __rfloordiv__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__rfloordiv__(extract_native(other)))

    def __pow__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__pow__(extract_native(other)))

    def __rpow__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__rpow__(extract_native(other)))

    def __mod__(self, other: PolarsExpr | Any) -> Self:
        return self._from_native_expr(self._expr.__mod__(extract_native(other)))

    def __rmod__(self, other: Any) -> Self:
        return self._from_native_expr(self._expr.__rmod__(extract_native(other)))

    # Unary

    def __invert__(self) -> Self:
        return self._from_native_expr(self._expr.__invert__())


    @property
    def str(self) -> PolarsExprStringNamespace:
        return PolarsExprStringNamespace(self)

    @property
    def dt(self) -> PolarsExprDateTimeNamespace:
        return PolarsExprDateTimeNamespace(self)

    @property
    def cat(self) -> PolarsExprCatNamespace:
        return PolarsExprCatNamespace(self)


class PolarsExprCatNamespace:
    def __init__(self, expr: PolarsExpr) -> None:
        self._expr = expr

    def get_categories(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr,
            "cat",
            "get_categories",
        )


class PolarsExprStringNamespace:
    def __init__(self, expr: PolarsExpr) -> None:
        self._expr = expr

    def starts_with(self, prefix: str) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr,
            "str",
            "starts_with",
            prefix,
        )

    def ends_with(self, suffix: str) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr,
            "str",
            "ends_with",
            suffix,
        )

    def contains(self, pattern: str, *, literal: bool) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr,
            "str",
            "contains",
            pattern,
            literal=literal,
        )

    def slice(self, offset: int, length: int | None = None) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr, "str", "slice", offset, length
        )

    def to_datetime(self, format: str | None = None) -> PolarsExpr:  # noqa: A002
        return reuse_series_namespace_implementation(
            self._expr,
            "str",
            "to_datetime",
            format,
        )

    def to_uppercase(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr,
            "str",
            "to_uppercase",
        )

    def to_lowercase(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr,
            "str",
            "to_lowercase",
        )


class PolarsExprDateTimeNamespace:
    def __init__(self, expr: PolarsExpr) -> None:
        self._expr = expr

    def year(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "year")

    def month(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "month")

    def day(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "day")

    def hour(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "hour")

    def minute(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "minute")

    def second(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "second")

    def millisecond(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "millisecond")

    def microsecond(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "microsecond")

    def nanosecond(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "nanosecond")

    def ordinal_day(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "ordinal_day")

    def total_minutes(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "total_minutes")

    def total_seconds(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(self._expr, "dt", "total_seconds")

    def total_milliseconds(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr, "dt", "total_milliseconds"
        )

    def total_microseconds(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr, "dt", "total_microseconds"
        )

    def total_nanoseconds(self) -> PolarsExpr:
        return reuse_series_namespace_implementation(
            self._expr, "dt", "total_nanoseconds"
        )

    def to_string(self, format: str) -> PolarsExpr:  # noqa: A002
        return reuse_series_namespace_implementation(
            self._expr, "dt", "to_string", format
        )
