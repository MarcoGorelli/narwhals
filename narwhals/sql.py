from __future__ import annotations
from typing import TYPE_CHECKING

from narwhals.dataframe import LazyFrame
from narwhals._spark_like.utils import narwhals_to_native_dtype
from narwhals.utils import Version
from narwhals.translate import from_native


if TYPE_CHECKING:
    from sqlframe.standalone import StandaloneDataFrame

def table(name: str, schema) -> LazyFrame[StandaloneDataFrame]:
    from sqlframe.standalone import StandaloneSession
    from sqlframe.standalone import types
    session = StandaloneSession.builder.getOrCreate()
    session.catalog.add_table(name, column_mapping={col: str(narwhals_to_native_dtype(dtype, Version.MAIN, types)) for col, dtype in schema.items()})
    return from_native(session.read.table(name))
