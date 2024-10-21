from __future__ import annotations

import pytest

import narwhals.stable.v1 as nw
from tests.utils import Constructor
from tests.utils import ConstructorEager
from tests.utils import assert_equal_data


def test_replace_strict(constructor: Constructor, request) -> None:
    if "dask" in str(constructor):
        request.applymarker(pytest.mark.xfail)
    df = nw.from_native(constructor({"a": [1, 2, 3]}))
    result = df.select(
        nw.col("a").replace_strict(
            {1: "one", 2: "two", 3: "three"}, default=None, return_dtype=nw.String
        )
    )
    assert_equal_data(result, {"a": ["one", "two", "three"]})


def test_replace_with_default(constructor: Constructor, request) -> None:
    if "dask" in str(constructor):
        request.applymarker(pytest.mark.xfail)
    df = nw.from_native(constructor({"a": [1, 2, 3]}))
    result = df.select(
        nw.col("a").replace_strict({1: 3, 3: 4}, default=999, return_dtype=nw.Int64)
    )


def test_replace_series(constructor_eager: ConstructorEager) -> None:
    s = nw.from_native(constructor_eager({"a": [1, 2, 3]}))["a"]
    result = s.replace({1: 3, 3: 4})
    assert_equal_data({"a": result}, {"a": [3, 2, 4]})
