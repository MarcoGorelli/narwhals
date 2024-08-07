from typing import Any

import pytest

import narwhals.stable.v1 as nw
from tests.utils import compare_dicts

data = {
    "a": [0.0, None, 2, 3, 4],
    "b": [1.0, None, None, 5, 3],
    "c": [5.0, None, 3, 2, 1],
}


def test_fill_null(request: Any, constructor: Any) -> None:
    if "pyarrow_table" in str(constructor):
        request.applymarker(pytest.mark.xfail)

    df = nw.from_native(constructor(data), eager_only=True)

    result = df.with_columns(nw.col("a", "b", "c").fill_null(99))
    expected = {
        "a": [0.0, 99, 2, 3, 4],
        "b": [1.0, 99, 99, 5, 3],
        "c": [5.0, 99, 3, 2, 1],
    }
    compare_dicts(result, expected)

    result = df.with_columns(
        a=df["a"].fill_null(99),
        b=df["b"].fill_null(99),
        c=df["c"].fill_null(99),
    )
    compare_dicts(result, expected)
