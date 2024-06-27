from typing import Any

from narwhals.dataframe import DataFrame as NarwhalsDataFrame
from narwhals.expression import Expr as NarwhalsExpr
from narwhals.expression import col as nw_col
from narwhals.series import Series as NarwhalsSeries
from narwhals.translate import from_native as nw_from_native
from narwhals.translate import to_native


class Expr(NarwhalsExpr): ...


class DataFrame(NarwhalsDataFrame): ...


class Series(NarwhalsSeries): ...


# todo: overload to get correct return dtype
def to_stable(obj: Any) -> Any:
    if isinstance(obj, NarwhalsDataFrame):
        return DataFrame(obj, is_polars=obj._is_polars)
    if isinstance(obj, NarwhalsSeries):
        return Series(obj, is_polars=obj._is_polars)
    if isinstance(obj, NarwhalsExpr):
        return Expr(obj._call)
    raise AssertionError


def from_native(obj: Any, *args: Any, **kwargs: Any) -> Any:
    return to_stable(nw_from_native(obj, *args, **kwargs))


def col(*args: Any, **kwargs: Any) -> Any:
    return to_stable(nw_col(*args, **kwargs))


__all__ = ["from_native", "col", "to_native", "DataFrame", "Series", "Expr"]
