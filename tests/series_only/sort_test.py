from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any

import pytest

import narwhals as nw
from tests.utils import assert_equal_data, skip_if_no_categorical_ordering

if TYPE_CHECKING:
    from tests.utils import ConstructorEager


@pytest.mark.parametrize(
    ("descending", "nulls_last", "expected"),
    [
        (True, True, [3, 2, 1, None]),
        (True, False, [None, 3, 2, 1]),
        (False, True, [1, 2, 3, None]),
        (False, False, [None, 1, 2, 3]),
    ],
)
def test_sort_series(
    constructor_eager: ConstructorEager, descending: Any, nulls_last: Any, expected: Any
) -> None:
    series = nw.from_native(constructor_eager({"a": [1, 3, 2, None]}), eager_only=True)[
        "a"
    ]
    result = series.sort(descending=descending, nulls_last=nulls_last)

    assert_equal_data({"a": result}, {"a": expected})


def test_sort_series_categorical(constructor_eager: ConstructorEager) -> None:
    # https://github.com/narwhals-dev/narwhals/issues/3841
    skip_if_no_categorical_ordering(constructor_eager)
    series = nw.from_native(
        constructor_eager({"a": ["dog", "bird", "cat"]}), eager_only=True
    )["a"].cast(nw.Categorical())
    assert_equal_data({"a": series.sort()}, {"a": ["bird", "cat", "dog"]})
    assert_equal_data({"a": series.sort(descending=True)}, {"a": ["dog", "cat", "bird"]})


@pytest.mark.parametrize(
    ("descending", "nulls_last", "expected"),
    [
        (False, False, [None, 1.5, 3.0, float("nan"), float("nan")]),
        (False, True, [1.5, 3.0, float("nan"), float("nan"), None]),
        (True, False, [None, float("nan"), float("nan"), 3.0, 1.5]),
        (True, True, [float("nan"), float("nan"), 3.0, 1.5, None]),
    ],
)
def test_sort_series_nan(
    constructor_eager: ConstructorEager,
    request: pytest.FixtureRequest,
    *,
    descending: bool,
    nulls_last: bool,
    expected: list[float | None],
) -> None:
    if any(
        x in str(constructor_eager)
        for x in ("pandas_constructor", "modin_constructor", "dask", "cudf")
    ):
        request.applymarker(
            pytest.mark.xfail(reason="NaN is null in non-nullable pandas")
        )
    if any(
        x in str(constructor_eager)
        for x in ("pandas_nullable", "pandas_pyarrow", "modin_pyarrow")
    ):
        request.applymarker(
            pytest.mark.xfail(reason="The constructor turns NaN into null")
        )
    data = {"a": [1.5, float("nan"), 3.0, None, float("nan")]}
    series = nw.from_native(constructor_eager(data), eager_only=True)["a"]
    result = series.sort(descending=descending, nulls_last=nulls_last)
    assert_equal_data({"a": result}, {"a": expected})
    is_nan = [None if x is None else math.isnan(x) for x in expected]
    assert_equal_data({"a": result.is_nan()}, {"a": is_nan})
