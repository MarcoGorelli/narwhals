from __future__ import annotations

from typing import Any, Generic

from narwhals._compliant import LazyExprNamespace
from narwhals._compliant.any_namespace import DateTimeNamespace
from narwhals._sql.typing import SQLExprT
from narwhals._utils import not_implemented


class SQLExprDateTimeNamespace(
    LazyExprNamespace["SQLExprT"], DateTimeNamespace["SQLExprT"], Generic[SQLExprT]
):
    def _with_elementwise(self, name: str, *args: Any) -> SQLExprT:
        comp = self.compliant
        return comp._with_elementwise(lambda expr: comp._function(name, expr, *args))

    def year(self) -> SQLExprT:
        return self._with_elementwise("year")

    def month(self) -> SQLExprT:
        return self._with_elementwise("month")

    def day(self) -> SQLExprT:
        return self._with_elementwise("day")

    def hour(self) -> SQLExprT:
        return self._with_elementwise("hour")

    def minute(self) -> SQLExprT:
        return self._with_elementwise("minute")

    def second(self) -> SQLExprT:
        return self._with_elementwise("second")

    def to_string(self, format: str) -> SQLExprT:
        return self._with_elementwise("strftime", self.compliant._lit(format))

    def ordinal_day(self) -> SQLExprT:
        return self._with_elementwise("dayofyear")

    timestamp = not_implemented()
