from __future__ import annotations

from typing import Any

from typing_extensions import Self

import narwhals as nw
from narwhals._compliant.dataframe import CompliantLazyFrame
from narwhals._compliant.expr import CompliantExpr
from narwhals.dataframe import LazyFrame
from narwhals.utils import Implementation, Version


class DictFrame(CompliantLazyFrame["DictExpr", "DictFrame", LazyFrame["DictFrame"]]):
    _implementation = Implementation.UNKNOWN

    def __init__(self, data, version=Version.MAIN):
        self.data = data
        self._version = version

    def __narwhals_lazyframe__(self):
        return self

    def __narwhals_namespace__(self):
        return DictNamespace(self)

    def _with_version(self, version):
        return DictFrame(self.data, version=version)


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


class DictNamespace:
    def __init__(self, version: Version):
        self._version = version

    def __narwhals_namespace__(self):
        return self

    def col(self, name: str):
        return DictExpr(
            lambda df: [self._df[name]],
            evaluate_output_names=lambda df: [name],
            alias_output_names=None,
            version=self._version,
        )


nw.from_native(DictFrame({"a": [1, 2, 3]})).with_columns(b=nw.col("a") * 2)
