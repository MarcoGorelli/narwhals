from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any

from narwhals._duckdb.dataframe import map_duckdb_dtype_to_narwhals_dtype

if TYPE_CHECKING:
    from narwhals.dtypes import DType
    from narwhals.typing import DTypes


class DuckDBInterchangeSeries:
    def __init__(self, df: Any) -> None:
        self._native_series = df

    def __narwhals_series__(self) -> Any:
        return self

    def dtype(self, dtypes: DTypes) -> DType:
        return map_duckdb_dtype_to_narwhals_dtype(self._native_series.types[0], dtypes)

    def __getattr__(self, attr: str) -> Any:
        msg = (  # pragma: no cover
            f"Attribute {attr} is not supported for metadata-only dataframes.\n\n"
            "If you would like to see this kind of object better supported in "
            "Narwhals, please open a feature request "
            "at https://github.com/narwhals-dev/narwhals/issues."
        )
        raise NotImplementedError(msg)  # pragma: no cover
