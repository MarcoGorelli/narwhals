from __future__ import annotations

import collections
from typing import TYPE_CHECKING
from typing import Any
from typing import Iterable
from typing import Iterator
from typing import Literal
from typing import Sequence
from typing import overload

from narwhals._expression_parsing import evaluate_into_exprs
from narwhals._pandas_like.expr import PandasLikeExpr
from narwhals._pandas_like.utils import Implementation
from narwhals._pandas_like.utils import create_native_series
from narwhals._pandas_like.utils import generate_unique_token
from narwhals._pandas_like.utils import horizontal_concat
from narwhals._pandas_like.utils import translate_dtype
from narwhals._pandas_like.utils import validate_dataframe_comparand
from narwhals._pandas_like.utils import validate_indices
from narwhals.dependencies import get_cudf
from narwhals.dependencies import get_dask
from narwhals.dependencies import get_modin
from narwhals.dependencies import get_numpy
from narwhals.dependencies import get_pandas
from narwhals.utils import flatten

if TYPE_CHECKING:
    from typing_extensions import Self

    from narwhals._pandas_like.group_by import PandasLikeGroupBy
    from narwhals._pandas_like.namespace import PandasLikeNamespace
    from narwhals._pandas_like.series import PandasLikeSeries
    from narwhals._pandas_like.typing import IntoPandasLikeExpr
    from narwhals.dtypes import DType


class PandasLikeDataFrame:
    # --- not in the spec ---
    def __init__(
        self,
        native_dataframe: Any,
        *,
        implementation: Implementation,
        backend_version: tuple[int, ...],
    ) -> None:
        self._validate_columns(native_dataframe.columns)
        self._native_dataframe = native_dataframe
        self._implementation = implementation
        self._backend_version = backend_version

    def __narwhals_dataframe__(self) -> Self:
        return self

    def __narwhals_lazyframe__(self) -> Self:
        return self

    def __narwhals_namespace__(self) -> PandasLikeNamespace:
        from narwhals._pandas_like.namespace import PandasLikeNamespace

        return PandasLikeNamespace(self._implementation, self._backend_version)

    def __native_namespace__(self) -> Any:
        if self._implementation is Implementation.PANDAS:
            return get_pandas()
        if self._implementation is Implementation.MODIN:  # pragma: no cover
            return get_modin()
        if self._implementation is Implementation.CUDF:  # pragma: no cover
            return get_cudf()
        if self._implementation is Implementation.DASK:  # pragma: no cover
            return get_dask()
        msg = f"Expected pandas/modin/cudf/dask, got: {type(self._implementation)}"  # pragma: no cover
        raise AssertionError(msg)

    def __len__(self) -> int:
        return len(self._native_dataframe)

    def _validate_columns(self, columns: Sequence[str]) -> None:
        if len(columns) != len(set(columns)):
            counter = collections.Counter(columns)
            for col, count in counter.items():
                if count > 1:
                    msg = f"Expected unique column names, got {col!r} {count} time(s)"
                    raise ValueError(msg)
            msg = "Please report a bug"  # pragma: no cover
            raise AssertionError(msg)

    def _from_native_dataframe(self, df: Any) -> Self:
        return self.__class__(
            df,
            implementation=self._implementation,
            backend_version=self._backend_version,
        )

    def get_column(self, name: str) -> PandasLikeSeries:
        from narwhals._pandas_like.series import PandasLikeSeries

        return PandasLikeSeries(
            self._native_dataframe.loc[:, name],
            implementation=self._implementation,
            backend_version=self._backend_version,
        )

    @overload
    def __getitem__(self, item: tuple[Sequence[int], str | int]) -> PandasLikeSeries: ...  # type: ignore[overload-overlap]

    @overload
    def __getitem__(self, item: Sequence[int]) -> PandasLikeDataFrame: ...

    @overload
    def __getitem__(self, item: str) -> PandasLikeSeries: ...

    @overload
    def __getitem__(self, item: slice) -> PandasLikeDataFrame: ...

    def __getitem__(
        self, item: str | slice | Sequence[int] | tuple[Sequence[int], str | int]
    ) -> PandasLikeSeries | PandasLikeDataFrame:
        if isinstance(item, str):
            from narwhals._pandas_like.series import PandasLikeSeries

            return PandasLikeSeries(
                self._native_dataframe.loc[:, item],
                implementation=self._implementation,
                backend_version=self._backend_version,
            )

        elif isinstance(item, tuple) and len(item) == 2:
            from narwhals._pandas_like.series import PandasLikeSeries

            if isinstance(item[1], str):
                native_series = self._native_dataframe.loc[item]
            elif isinstance(item[1], int):
                native_series = self._native_dataframe.iloc[item]
            else:  # pragma: no cover
                msg = f"Expected str or int, got: {type(item[1])}"
                raise TypeError(msg)

            return PandasLikeSeries(
                native_series,
                implementation=self._implementation,
                backend_version=self._backend_version,
            )

        elif isinstance(item, (slice, Sequence)) or (
            (np := get_numpy()) is not None
            and isinstance(item, np.ndarray)
            and item.ndim == 1
        ):
            return self._from_native_dataframe(self._native_dataframe.iloc[item])

        else:  # pragma: no cover
            msg = f"Expected str or slice, got: {type(item)}"
            raise TypeError(msg)

    # --- properties ---
    @property
    def columns(self) -> list[str]:
        return self._native_dataframe.columns.tolist()  # type: ignore[no-any-return]

    def rows(
        self, *, named: bool = False
    ) -> list[tuple[Any, ...]] | list[dict[str, Any]]:
        if not named:
            return list(self._native_dataframe.itertuples(index=False, name=None))

        return self._native_dataframe.to_dict(orient="records")  # type: ignore[no-any-return]

    def iter_rows(
        self,
        *,
        named: bool = False,
        buffer_size: int = 512,  # noqa: ARG002
    ) -> Iterator[list[tuple[Any, ...]]] | Iterator[list[dict[str, Any]]]:
        """
        NOTE:
            The param ``buffer_size`` is only here for compatibility with the polars API
            and has no effect on the output.
        """
        if not named:
            yield from self._native_dataframe.itertuples(index=False, name=None)
        else:
            yield from (
                row._asdict() for row in self._native_dataframe.itertuples(index=False)
            )

    @property
    def schema(self) -> dict[str, DType]:
        return {
            col: translate_dtype(self._native_dataframe.loc[:, col])
            for col in self._native_dataframe.columns
        }

    def collect_schema(self) -> dict[str, DType]:
        return self.schema

    # --- reshape ---
    def select(
        self,
        *exprs: IntoPandasLikeExpr,
        **named_exprs: IntoPandasLikeExpr,
    ) -> Self:
        new_series = evaluate_into_exprs(self, *exprs, **named_exprs)
        if not new_series:
            # return empty dataframe, like Polars does
            if self._implementation is Implementation.DASK:
                dd = get_dask()
                return self._from_native_dataframe(dd.from_dict({}, npartitions=1))
            return self._from_native_dataframe(self._native_dataframe.__class__())
        new_series = validate_indices(new_series)
        df = horizontal_concat(
            new_series,
            implementation=self._implementation,
            backend_version=self._backend_version,
        )
        return self._from_native_dataframe(df)

    def drop_nulls(self) -> Self:
        return self._from_native_dataframe(self._native_dataframe.dropna(axis=0))

    def with_row_index(self, name: str) -> Self:
        row_index = create_native_series(
            range(len(self._native_dataframe)),
            index=self._native_dataframe.index,
            implementation=self._implementation,
            backend_version=self._backend_version,
        ).alias(name)
        return self._from_native_dataframe(
            horizontal_concat(
                [row_index._native_series, self._native_dataframe],
                implementation=self._implementation,
                backend_version=self._backend_version,
            )
        )

    def filter(
        self,
        *predicates: IntoPandasLikeExpr,
    ) -> Self:
        from narwhals._pandas_like.namespace import PandasLikeNamespace

        plx = PandasLikeNamespace(self._implementation, self._backend_version)
        expr = plx.all_horizontal(*predicates)
        # Safety: all_horizontal's expression only returns a single column.
        mask = expr._call(self)[0]
        _mask = validate_dataframe_comparand(self._native_dataframe.index, mask)
        return self._from_native_dataframe(self._native_dataframe.loc[_mask])

    def with_columns(
        self,
        *exprs: IntoPandasLikeExpr,
        **named_exprs: IntoPandasLikeExpr,
    ) -> Self:
        index = self._native_dataframe.index
        new_columns = evaluate_into_exprs(self, *exprs, **named_exprs)
        # If the inputs are all Expressions which return full columns
        # (as opposed to scalars), we can use a fast path (concat, instead of assign).
        # We can't use the fastpath if any input is not an expression (e.g.
        # if it's a Series) because then we might be changing its flags.
        # See `test_memmap` for an example of where this is necessary.
        fast_path = (
            all(len(s) > 1 for s in new_columns)
            and all(isinstance(x, PandasLikeExpr) for x in exprs)
            and all(isinstance(x, PandasLikeExpr) for (_, x) in named_exprs.items())
        )

        if fast_path:
            new_column_name_to_new_column_map = {s.name: s for s in new_columns}
            to_concat = []
            # Make sure to preserve column order
            for name in self._native_dataframe.columns:
                if name in new_column_name_to_new_column_map:
                    to_concat.append(
                        validate_dataframe_comparand(
                            index, new_column_name_to_new_column_map.pop(name)
                        )
                    )
                else:
                    to_concat.append(self._native_dataframe.loc[:, name])
            to_concat.extend(
                validate_dataframe_comparand(index, new_column_name_to_new_column_map[s])
                for s in new_column_name_to_new_column_map
            )

            df = horizontal_concat(
                to_concat,
                implementation=self._implementation,
                backend_version=self._backend_version,
            )
        else:
            df = self._native_dataframe.assign(
                **{s.name: validate_dataframe_comparand(index, s) for s in new_columns}
            )
        return self._from_native_dataframe(df)

    def rename(self, mapping: dict[str, str]) -> Self:
        return self._from_native_dataframe(self._native_dataframe.rename(columns=mapping))

    def drop(self, *columns: str | Iterable[str]) -> Self:
        return self._from_native_dataframe(
            self._native_dataframe.drop(columns=list(flatten(columns)))
        )

    # --- transform ---
    def sort(
        self,
        by: str | Iterable[str],
        *more_by: str,
        descending: bool | Sequence[bool] = False,
    ) -> Self:
        flat_keys = flatten([*flatten([by]), *more_by])
        df = self._native_dataframe
        if isinstance(descending, bool):
            ascending: bool | list[bool] = not descending
        else:
            ascending = [not d for d in descending]
        return self._from_native_dataframe(df.sort_values(flat_keys, ascending=ascending))

    # --- convert ---
    def collect(self) -> PandasLikeDataFrame:
        if self._implementation is Implementation.DASK:
            return_df = self._native_dataframe.compute()
            return_implementation = Implementation.PANDAS
        else:
            return_df = self._native_dataframe
            return_implementation = self._implementation
        return PandasLikeDataFrame(
            return_df,
            implementation=return_implementation,
            backend_version=self._backend_version,
        )

    # --- actions ---
    def group_by(self, *keys: str | Iterable[str]) -> PandasLikeGroupBy:
        from narwhals._pandas_like.group_by import PandasLikeGroupBy

        return PandasLikeGroupBy(
            self,
            flatten(keys),
        )

    def join(
        self,
        other: Self,
        *,
        how: Literal["left", "inner", "outer", "cross", "anti", "semi"] = "inner",
        left_on: str | list[str] | None,
        right_on: str | list[str] | None,
    ) -> Self:
        if isinstance(left_on, str):
            left_on = [left_on]
        if isinstance(right_on, str):
            right_on = [right_on]

        if how == "cross":
            if (
                self._implementation is Implementation.MODIN
                or self._implementation is Implementation.CUDF
            ) or (
                self._implementation is Implementation.PANDAS
                and self._backend_version < (1, 4)
            ):
                key_token = generate_unique_token(
                    n_bytes=8, columns=[*self.columns, *other.columns]
                )

                return self._from_native_dataframe(
                    self._native_dataframe.assign(**{key_token: 0}).merge(
                        other._native_dataframe.assign(**{key_token: 0}),
                        how="inner",
                        left_on=key_token,
                        right_on=key_token,
                        suffixes=("", "_right"),
                    ),
                ).drop(key_token)
            else:
                return self._from_native_dataframe(
                    self._native_dataframe.merge(
                        other._native_dataframe,
                        how="cross",
                        suffixes=("", "_right"),
                    ),
                )

        if how == "anti":
            indicator_token = generate_unique_token(
                n_bytes=8, columns=[*self.columns, *other.columns]
            )

            other_native = (
                other._native_dataframe.loc[:, right_on]
                .rename(  # rename to avoid creating extra columns in join
                    columns=dict(zip(right_on, left_on))  # type: ignore[arg-type]
                )
                .drop_duplicates()
            )
            return self._from_native_dataframe(
                self._native_dataframe.merge(
                    other_native,
                    how="outer",
                    indicator=indicator_token,
                    left_on=left_on,
                    right_on=left_on,
                )
                .loc[lambda t: t[indicator_token] == "left_only"]
                .drop(columns=[indicator_token])
            )

        if how == "semi":
            other_native = (
                other._native_dataframe.loc[:, right_on]
                .rename(  # rename to avoid creating extra columns in join
                    columns=dict(zip(right_on, left_on))  # type: ignore[arg-type]
                )
                .drop_duplicates()  # avoids potential rows duplication from inner join
            )
            return self._from_native_dataframe(
                self._native_dataframe.merge(
                    other_native,
                    how="inner",
                    left_on=left_on,
                    right_on=left_on,
                )
            )

        if how == "left":
            other_native = other._native_dataframe
            result_native = self._native_dataframe.merge(
                other_native,
                how="left",
                left_on=left_on,
                right_on=right_on,
                suffixes=("", "_right"),
            )
            extra = []
            for left_key, right_key in zip(left_on, right_on):  # type: ignore[arg-type]
                if right_key != left_key and right_key not in self.columns:
                    extra.append(right_key)
                elif right_key != left_key:
                    extra.append(f"{right_key}_right")
            return self._from_native_dataframe(result_native.drop(columns=extra))

        return self._from_native_dataframe(
            self._native_dataframe.merge(
                other._native_dataframe,
                left_on=left_on,
                right_on=right_on,
                how=how,
                suffixes=("", "_right"),
            ),
        )

    # --- partial reduction ---

    def head(self, n: int) -> Self:
        return self._from_native_dataframe(self._native_dataframe.head(n))

    def tail(self, n: int) -> Self:
        return self._from_native_dataframe(self._native_dataframe.tail(n))

    def unique(self, subset: str | list[str]) -> Self:
        subset = flatten(subset)
        return self._from_native_dataframe(
            self._native_dataframe.drop_duplicates(subset=subset)
        )

    # --- lazy-only ---
    def lazy(self) -> Self:
        return self

    @property
    def shape(self) -> tuple[int, int]:
        return self._native_dataframe.shape  # type: ignore[no-any-return]

    def to_dict(self, *, as_series: bool = False) -> dict[str, Any]:
        from narwhals._pandas_like.series import PandasLikeSeries

        if as_series:
            # TODO(Unassigned): should this return narwhals series?
            return {
                col: PandasLikeSeries(
                    self._native_dataframe.loc[:, col],
                    implementation=self._implementation,
                    backend_version=self._backend_version,
                )
                for col in self.columns
            }
        return self._native_dataframe.to_dict(orient="list")  # type: ignore[no-any-return]

    def to_numpy(self) -> Any:
        from narwhals._pandas_like.series import PANDAS_TO_NUMPY_DTYPE_MISSING

        # pandas return `object` dtype for nullable dtypes, so we cast each
        # Series to numpy and let numpy find a common dtype.
        # If there aren't any dtypes where `to_numpy()` is "broken" (i.e. it
        # returns Object) then we just call `to_numpy()` on the DataFrame.
        for dtype in self._native_dataframe.dtypes:
            if str(dtype) in PANDAS_TO_NUMPY_DTYPE_MISSING:
                import numpy as np

                return np.hstack([self[col].to_numpy()[:, None] for col in self.columns])
        if self._implementation is Implementation.DASK:
            return self._native_dataframe.compute().to_numpy()
        return self._native_dataframe.to_numpy()

    def to_pandas(self) -> Any:
        if self._implementation is Implementation.PANDAS:
            return self._native_dataframe
        if self._implementation is Implementation.MODIN:  # pragma: no cover
            return self._native_dataframe._to_pandas()
        if self._implementation is Implementation.DASK:  # pragma: no cover
            return self._native_dataframe.compute()
        return self._native_dataframe.to_pandas()  # pragma: no cover

    def write_parquet(self, file: Any) -> Any:
        self._native_dataframe.to_parquet(file)

    # --- descriptive ---
    def is_duplicated(self: Self) -> PandasLikeSeries:
        from narwhals._pandas_like.series import PandasLikeSeries

        return PandasLikeSeries(
            self._native_dataframe.duplicated(keep=False),
            implementation=self._implementation,
            backend_version=self._backend_version,
        )

    def is_empty(self: Self) -> bool:
        return self._native_dataframe.empty  # type: ignore[no-any-return]

    def is_unique(self: Self) -> PandasLikeSeries:
        from narwhals._pandas_like.series import PandasLikeSeries

        return PandasLikeSeries(
            ~self._native_dataframe.duplicated(keep=False),
            implementation=self._implementation,
            backend_version=self._backend_version,
        )

    def null_count(self: Self) -> PandasLikeDataFrame:
        return PandasLikeDataFrame(
            self._native_dataframe.isna().sum(axis=0).to_frame().transpose(),
            implementation=self._implementation,
            backend_version=self._backend_version,
        )

    def item(self: Self, row: int | None = None, column: int | str | None = None) -> Any:
        if row is None and column is None:
            if self.shape != (1, 1):
                msg = (
                    "can only call `.item()` if the dataframe is of shape (1, 1),"
                    " or if explicit row/col values are provided;"
                    f" frame has shape {self.shape!r}"
                )
                raise ValueError(msg)
            return self._native_dataframe.iloc[0, 0]

        elif row is None or column is None:
            msg = "cannot call `.item()` with only one of `row` or `column`"
            raise ValueError(msg)

        _col = self.columns.index(column) if isinstance(column, str) else column
        return self._native_dataframe.iloc[row, _col]

    def clone(self: Self) -> Self:
        return self._from_native_dataframe(self._native_dataframe.copy())
