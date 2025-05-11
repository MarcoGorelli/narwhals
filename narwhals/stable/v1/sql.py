from __future__ import annotations

from narwhals.stable.v1.utils import _stableify
from narwhals.sql import table as nw_table

def table(name: str, schema) -> LazyFrame[StandaloneDataFrame]:
    return _stableify(nw_table(name, schema))
