from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from narwhals._compliant.typing import NativeExprT
    from narwhals._compliant.window import WindowInputs
    from narwhals._sql.expr import SQLExpr
    from narwhals._sql.typing import SQLLazyFrameT


def evaluate_exprs(
    df: SQLLazyFrameT, /, *exprs: SQLExpr[SQLLazyFrameT, NativeExprT]
) -> list[NativeExprT]:
    native_results: list[NativeExprT] = []
    for expr in exprs:
        native_results.extend(expr(df))
    return native_results


def evaluate_exprs_in_window_function(
    df: SQLLazyFrameT,
    /,
    window_inputs: WindowInputs[NativeExprT],
    *exprs: SQLExpr[SQLLazyFrameT, NativeExprT],
) -> list[NativeExprT]:
    native_results: list[NativeExprT] = []
    for expr in exprs:
        native_results.extend(expr.window_function(df, window_inputs))
    return native_results
