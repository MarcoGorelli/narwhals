from __future__ import annotations

from functools import wraps
from typing import TYPE_CHECKING
from typing import Any
from typing import Callable
from typing import Iterable
from typing import Literal
from typing import Sequence
from typing import cast
from typing import overload
from warnings import warn

import narwhals as nw
from narwhals import dependencies
from narwhals import exceptions
from narwhals import selectors
from narwhals.dataframe import DataFrame as NwDataFrame
from narwhals.dataframe import LazyFrame as NwLazyFrame
from narwhals.dependencies import get_polars
from narwhals.exceptions import InvalidIntoExprError
from narwhals.expr import Expr as NwExpr
from narwhals.functions import Then as NwThen
from narwhals.functions import When as NwWhen
from narwhals.functions import _from_arrow_impl
from narwhals.functions import _from_dict_impl
from narwhals.functions import _from_numpy_impl
from narwhals.functions import _new_series_impl
from narwhals.functions import _read_csv_impl
from narwhals.functions import _read_parquet_impl
from narwhals.functions import _scan_csv_impl
from narwhals.functions import _scan_parquet_impl
from narwhals.functions import get_level
from narwhals.functions import show_versions
from narwhals.functions import when as nw_when
from narwhals.schema import Schema as NwSchema
from narwhals.series import Series as NwSeries
from narwhals.stable.v1 import dtypes
from narwhals.stable.v1.dtypes import Array
from narwhals.stable.v1.dtypes import Binary
from narwhals.stable.v1.dtypes import Boolean
from narwhals.stable.v1.dtypes import Categorical
from narwhals.stable.v1.dtypes import Date
from narwhals.stable.v1.dtypes import Datetime
from narwhals.stable.v1.dtypes import Decimal
from narwhals.stable.v1.dtypes import Duration
from narwhals.stable.v1.dtypes import Enum
from narwhals.stable.v1.dtypes import Field
from narwhals.stable.v1.dtypes import Float32
from narwhals.stable.v1.dtypes import Float64
from narwhals.stable.v1.dtypes import Int8
from narwhals.stable.v1.dtypes import Int16
from narwhals.stable.v1.dtypes import Int32
from narwhals.stable.v1.dtypes import Int64
from narwhals.stable.v1.dtypes import Int128
from narwhals.stable.v1.dtypes import List
from narwhals.stable.v1.dtypes import Object
from narwhals.stable.v1.dtypes import String
from narwhals.stable.v1.dtypes import Struct
from narwhals.stable.v1.dtypes import Time
from narwhals.stable.v1.dtypes import UInt8
from narwhals.stable.v1.dtypes import UInt16
from narwhals.stable.v1.dtypes import UInt32
from narwhals.stable.v1.dtypes import UInt64
from narwhals.stable.v1.dtypes import UInt128
from narwhals.stable.v1.dtypes import Unknown
from narwhals.translate import _from_native_impl
from narwhals.translate import get_native_namespace
from narwhals.translate import to_py_scalar
from narwhals.typing import IntoDataFrameT
from narwhals.typing import IntoFrameT
from narwhals.utils import Implementation
from narwhals.utils import Version
from narwhals.utils import deprecate_native_namespace
from narwhals.utils import find_stacklevel
from narwhals.utils import generate_temporary_column_name
from narwhals.utils import inherit_doc
from narwhals.utils import is_ordered_categorical
from narwhals.utils import maybe_align_index
from narwhals.utils import maybe_convert_dtypes
from narwhals.utils import maybe_get_index
from narwhals.utils import maybe_reset_index
from narwhals.utils import maybe_set_index
from narwhals.utils import validate_strict_and_pass_though

if TYPE_CHECKING:
    from types import ModuleType
    from typing import Mapping

    from typing_extensions import ParamSpec
    from typing_extensions import Self
    from typing_extensions import TypeVar

    from narwhals._translate import IntoArrowTable
    from narwhals.dataframe import MultiColSelector
    from narwhals.dataframe import MultiIndexSelector
    from narwhals.dtypes import DType
    from narwhals.typing import ConcatMethod
    from narwhals.typing import IntoExpr
    from narwhals.typing import IntoFrame
    from narwhals.typing import IntoLazyFrameT
    from narwhals.typing import IntoSeries
    from narwhals.typing import NonNestedLiteral
    from narwhals.typing import SingleColSelector
    from narwhals.typing import SingleIndexSelector
    from narwhals.typing import _1DArray
    from narwhals.typing import _2DArray

    FrameT = TypeVar("FrameT", "DataFrame[Any]", "LazyFrame[Any]")
    DataFrameT = TypeVar("DataFrameT", bound="DataFrame[Any]")
    LazyFrameT = TypeVar("LazyFrameT", bound="LazyFrame[Any]")
    SeriesT = TypeVar("SeriesT", bound="Series[Any]")
    IntoSeriesT = TypeVar("IntoSeriesT", bound="IntoSeries", default=Any)
    T = TypeVar("T", default=Any)
    P = ParamSpec("P")
    R = TypeVar("R")
else:
    from typing import TypeVar

    IntoSeriesT = TypeVar("IntoSeriesT", bound="IntoSeries")
    T = TypeVar("T")
from typing import overload

@overload
def _stableify(obj: NwDataFrame[IntoFrameT]) -> DataFrame[IntoFrameT]: ...
@overload
def _stableify(obj: NwLazyFrame[IntoFrameT]) -> LazyFrame[IntoFrameT]: ...
@overload
def _stableify(obj: NwSeries[IntoSeriesT]) -> Series[IntoSeriesT]: ...
@overload
def _stableify(obj: NwExpr) -> Expr: ...
@overload
def _stableify(obj: Any) -> Any: ...


def _stableify(
    obj: NwDataFrame[IntoFrameT]
    | NwLazyFrame[IntoFrameT]
    | NwSeries[IntoSeriesT]
    | NwExpr
    | Any,
) -> DataFrame[IntoFrameT] | LazyFrame[IntoFrameT] | Series[IntoSeriesT] | Expr | Any:
    if isinstance(obj, NwDataFrame):
        return DataFrame(obj._compliant_frame._with_version(Version.V1), level=obj._level)
    if isinstance(obj, NwLazyFrame):
        return LazyFrame(obj._compliant_frame._with_version(Version.V1), level=obj._level)
    if isinstance(obj, NwSeries):
        return Series(obj._compliant_series._with_version(Version.V1), level=obj._level)
    if isinstance(obj, NwExpr):
        return Expr(obj._to_compliant_expr, obj._metadata)
    return obj
