# ok, well, this isn't easy, but...it should be doable
# hard to prioritise. sklearn, or this?
# sklearn?

from __future__ import annotations

from typing import Any

from typing_extensions import Self

import narwhals as nw
from narwhals._compliant.dataframe import CompliantLazyFrame
from narwhals._compliant.expr import CompliantExpr
from narwhals._compliant.namespace import CompliantNamespace
from narwhals.dataframe import LazyFrame
from narwhals.utils import Implementation, Version


class DictFrame(CompliantLazyFrame["DictExpr", "DictFrame", LazyFrame["DictFrame"]]):
    _implementation = Implementation.UNKNOWN

    def __init__(self, data, version=Version.MAIN):
        self._native_frame = data
        self._version = version

    def __narwhals_lazyframe__(self):
        return self

    def __narwhals_namespace__(self):
        return DictNamespace(self)

    def _with_version(self, version):
        return DictFrame(self._native_frame, version=version)

    @property
    def columns(self):
        return list(self._native_frame.keys())

    def simple_select(self, *column_names: str) -> Self:
        return DictFrame(
            {key: val for key, val in self._native_frame.items() if key in column_names},
            version=self._version,
        )

    def select(self, *exprs: DictExpr) -> Self:
        results = []
        result_names = []
        for expr in exprs:
            results.extend(expr(self))
            result_names.extend(expr._evaluate_output_names(self))
        data = dict(zip(result_names, results))
        return DictFrame(data, version=self._version)


class DictExpr(CompliantExpr[DictFrame, Any]):
    _implementation = Implementation.UNKNOWN

    def __init__(
        self,
        call,
        evaluate_output_names: Any,
        alias_output_names: Any,
        version: Version,
        implementation=Implementation.UNKNOWN,
    ):
        self._call = call
        self._version = version
        self._evaluate_output_names = evaluate_output_names
        self._alias_output_names = alias_output_names
        self._metadata: Any = None

    @classmethod
    def from_column_names(cls, evaluate_column_names: Any, /, *, context: Any) -> Self:
        return DictExpr(
            lambda df: [df[x] for x in evaluate_column_names(df)],
            alias_output_names=None,
            version=context.version,
        )

    @classmethod
    def from_column_indices(cls, *column_indices: int, context: Any) -> Self:
        def func(df):
            names = df.data.keys()
            return [df.data[names[i]] for i in column_indices]

        return DictExpr(func, alias_output_names=None, version=context.version)

    def __narwhals_expr__(self) -> Self:
        return self

    def __narwhals_namespace__(self) -> DictNamespace:
        return DictNamespace(self._version)

    def __call__(self, df):
        return self._call(df)

    def __mul__(self, value):
        def func(df):
            exprs = self(df)
            return [[x * value for x in expr] for expr in exprs]

        return DictExpr(
            func,
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )


class DictNamespace(CompliantNamespace):
    def __init__(self, version: Version):
        self._version = version

    def __narwhals_namespace__(self):
        return self

    def col(self, name: str):
        return DictExpr(
            lambda df: [df._native_frame[name]],
            evaluate_output_names=lambda df: [name],
            alias_output_names=None,
            version=self._version,
        )


print(
    nw.from_native(DictFrame({"a": [1, 2, 3], "b": [4, 5, 6]})).select(
        nw.col("b") * 2, nw.col("a")
    )
)
