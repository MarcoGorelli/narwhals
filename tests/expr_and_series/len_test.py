from typing import Any

import pytest

import narwhals.stable.v1 as nw
from tests.utils import compare_dicts


def test_len(constructor: Any, request: Any) -> None:
    data = {"a": list("xyz"), "b": [1, 2, 1]}
    expected = {"a1": [2], "a2": [1]}
    if "dask" in str(constructor):
        request.applymarker(pytest.mark.xfail)
    df = nw.from_native(constructor(data)).select(
        nw.col("a").filter(nw.col("b") == 1).len().alias("a1"),
        nw.col("a").filter(nw.col("b") == 2).len().alias("a2"),
    )

    compare_dicts(df, expected)


def test_namespace_len(constructor: Any) -> None:
    df = nw.from_native(constructor({"a": [1, 2, 3], "b": [4, 5, 6]})).select(
        nw.len(), a=nw.len()
    )
    expected = {"len": [3], "a": [3]}
    compare_dicts(df, expected)
    df = (
        nw.from_native(constructor({"a": [1, 2, 3], "b": [4, 5, 6]}))
        .select()
        .select(nw.len(), a=nw.len())
    )
    expected = {"len": [0], "a": [0]}
    compare_dicts(df, expected)
