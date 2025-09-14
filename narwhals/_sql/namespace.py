from __future__ import annotations

import operator
from functools import reduce
from typing import TYPE_CHECKING, Any, Protocol

from narwhals._compliant import LazyNamespace
from narwhals._compliant.typing import NativeExprT, NativeFrameT_co
from narwhals._expression_parsing import is_compliant_expr
from narwhals._sql.typing import SQLExprT, SQLLazyFrameT

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from narwhals._sql.expr import WindowInputs
    from narwhals.typing import NonNestedLiteral, PythonLiteral


class SQLNamespace(
    LazyNamespace[SQLLazyFrameT, SQLExprT, NativeFrameT_co],
    Protocol[SQLLazyFrameT, SQLExprT, NativeFrameT_co, NativeExprT],
):
    def _function(self, name: str, *args: NativeExprT | PythonLiteral) -> NativeExprT: ...
    def _lit(self, value: Any) -> NativeExprT: ...
    def _when(
        self,
        condition: NativeExprT,
        value: NativeExprT,
        otherwise: NativeExprT | None = None,
    ) -> NativeExprT: ...
    def _coalesce(self, *exprs: NativeExprT) -> NativeExprT: ...

    # Horizontal functions
    def any_horizontal(self, *exprs: SQLExprT, ignore_nulls: bool) -> SQLExprT:
        def func(cols: Iterable[NativeExprT]) -> NativeExprT:
            if ignore_nulls:
                cols = (self._coalesce(col, self._lit(False)) for col in cols)
            return reduce(operator.or_, cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def all_horizontal(self, *exprs: SQLExprT, ignore_nulls: bool) -> SQLExprT:
        def func(cols: Iterable[NativeExprT]) -> NativeExprT:
            if ignore_nulls:
                cols = (self._coalesce(col, self._lit(True)) for col in cols)
            return reduce(operator.and_, cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def max_horizontal(self, *exprs: SQLExprT) -> SQLExprT:
        def func(cols: Iterable[NativeExprT]) -> NativeExprT:
            return self._function("greatest", *cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def min_horizontal(self, *exprs: SQLExprT) -> SQLExprT:
        def func(cols: Iterable[NativeExprT]) -> NativeExprT:
            return self._function("least", *cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def sum_horizontal(self, *exprs: SQLExprT) -> SQLExprT:
        def func(cols: Iterable[NativeExprT]) -> NativeExprT:
            return reduce(
                operator.add, (self._coalesce(col, self._lit(0)) for col in cols)
            )

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    # Other
    def coalesce(self, *exprs: SQLExprT) -> SQLExprT:
        def func(cols: Iterable[NativeExprT]) -> NativeExprT:
            return self._coalesce(*cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def when_then(
        self,
        predicate: SQLExprT,
        then: SQLExprT | NonNestedLiteral,
        otherwise: SQLExprT | NonNestedLiteral | None = None,
    ) -> SQLExprT:
        def call(df: SQLLazyFrameT) -> Sequence[NativeExprT]:
            then_native = (
                df._evaluate_expr(then) if is_compliant_expr(then) else self._lit(then)
            )
            otherwise_native = (
                df._evaluate_expr(otherwise)
                if is_compliant_expr(otherwise)
                else None
                if otherwise is None
                else self._lit(otherwise)
            )

            return [
                self._when(df._evaluate_expr(predicate), then_native, otherwise_native)
            ]

        def window_function(
            df: SQLLazyFrameT, window_inputs: WindowInputs[NativeExprT]
        ) -> Sequence[NativeExprT]:
            then_native = (
                df._evaluate_window_expr(then, window_inputs)
                if is_compliant_expr(then)
                else self._lit(then)
            )
            otherwise_native = (
                df._evaluate_window_expr(otherwise, window_inputs)
                if is_compliant_expr(otherwise)
                else None
                if otherwise is None
                else self._lit(otherwise)
            )

            return [
                self._when(
                    df._evaluate_window_expr(predicate, window_inputs),
                    then_native,
                    otherwise_native,
                )
            ]

        context = predicate
        return self._expr(
            call,
            window_function=window_function,
            evaluate_output_names=getattr(
                then, "_evaluate_output_names", lambda _df: ["literal"]
            ),
            alias_output_names=getattr(then, "_alias_output_names", None),
            version=context._version,
            implementation=context._implementation,
        )
