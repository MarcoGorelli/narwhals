from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar, Protocol

from narwhals._compliant.expr import NativeExpr

if TYPE_CHECKING:
    from typing_extensions import Self


class NativeSQLExpr(NativeExpr, Protocol):
    # both Self because we're comparing an expression with an expression?
    # def __gt__(self, value: Self) -> Self: ...

    # def __lt__(self, value: Self) -> Self: ...

    def __ge__(self, value: Any, /) -> Self: ...

    # def __le__(self, value: Self) -> Self: ...

    # def __eq__(self, value: Self) -> Self: ...

    # def __ne__(self, value: Self) -> Self: ...
    # # do we want any more of the arithmetic methods? I wasn't sure between lefthan & righthand methods..
    # def __sub__(self, value: Self) -> Self: ...

    # def __add__(self, value: Self) -> Self: ...

    # def __truediv__(self, value: Self) -> Self: ...

    # def __mul__(self, value: Self) -> Self: ...

    # def __invert__(self) -> Self: ...


if TYPE_CHECKING:
    from typing_extensions import Self

    from narwhals._sql.dataframe import SQLLazyFrame
    from narwhals._sql.expr import SQLExpr

    # TODO: @mp, understand why these are here & if we need one for NativeSQLExprT;
    # seem to reflect number of different 'catgories' each of the parent class has
    # tbc! since NativeExpr only has Protocol, I don't think we need this for NativeSQLExpr
    # NativeSQLExpr isn't accepting Any arguments :) I need to go back to the reading on
    # cov-, contra- & invariance
    SQLExprAny = SQLExpr[Any, Any]
    SQLLazyFrameAny = SQLLazyFrame[Any, Any, Any]
    NativeSQLExprAny = NativeSQLExpr

SQLExprT = TypeVar("SQLExprT", bound="SQLExprAny")
SQLExprT_contra = TypeVar("SQLExprT_contra", bound="SQLExprAny", contravariant=True)
SQLLazyFrameT = TypeVar("SQLLazyFrameT", bound="SQLLazyFrameAny")
# TODO: @mp, should this be contravariant as to do with function arguments? think through!
NativeSQLExprT = TypeVar("NativeSQLExprT", bound="NativeSQLExpr")
