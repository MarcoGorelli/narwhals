from __future__ import annotations

import pytest

import narwhals as nw
from narwhals.exceptions import InvalidOperationError
from tests.utils import Constructor, assert_equal_data


@pytest.mark.parametrize(
    ("expr", "expected"),
    [
        (nw.col("a"), [-1, 2, 3]),
        (nw.col("a").mean(), [1.333333333]),
        (nw.col("a").cum_sum().over(order_by="i"), [-1, 1, 4]),
        (nw.col("a").cum_sum().abs().over(order_by="i"), [1, 1, 4]),
        ((nw.col("a").cum_sum() + 1).over(order_by="i"), [0, 2, 5]),
        (
            nw.sum_horizontal(nw.col("a"), nw.col("a").cum_sum()).over(order_by="a"),
            [-2, 3, 7],
        ),
        (
            nw.sum_horizontal(nw.col("a"), nw.col("a").cum_sum().over(order_by="i")),
            [-2, 3, 7],
        ),
        (
            nw.sum_horizontal(nw.col("a").diff(), nw.col("a").cum_sum()).over(
                order_by="i"
            ),
            [-1.0, 4.0, 5.0],
        ),
        (
            nw.sum_horizontal(nw.col("a").diff().abs(), nw.col("a").cum_sum()).over(
                order_by="i"
            ),
            [-1.0, 4.0, 5.0],
        ),
    ],
)
def test_over_pushdown(
    constructor: Constructor, expr: nw.Expr, expected: list[float]
) -> None:
    df = nw.from_native(constructor({"a": [-1, 2, 3], "i": [0, 1, 2]})).lazy()
    result = df.select(a=expr)
    assert_equal_data(result, {"a": expected})


@pytest.mark.parametrize(
    "expr",
    [
        nw.col("a").cum_sum(),
        nw.col("a").cum_sum().cum_sum().over(order_by="i"),
        nw.col("a").cum_sum().cum_sum(),
        nw.sum_horizontal(nw.col("a"), nw.col("a").cum_sum()),
        nw.sum_horizontal(nw.col("a").diff(), nw.col("a").cum_sum().over(order_by="i")),
        nw.col("a").mean().over(order_by="i"),
        nw.col("a").mean().over("b").over("c"),
        nw.col("a").mean().over("b").over("c", order_by="i"),
        nw.col("a").mean().mean(),
        nw.col("a").mean().sum(),
        nw.col("a").mean().drop_nulls(),
        nw.col("a").mean().rank(),
        nw.col("a").mean().is_unique(),
        nw.col("a").mean().diff(),
        nw.col("a").fill_null(3).over("b"),
        nw.col("a").drop_nulls().over("b"),
        nw.col("a").drop_nulls().over("b", order_by="i"),
        nw.col("a").diff().drop_nulls().over("b", order_by="i"),
    ],
)
def test_invalid_operations(constructor: Constructor, expr: nw.Expr) -> None:
    df = nw.from_native(
        constructor({"a": [-1, 2, 3], "b": [1, 1, 1], "c": [2, 2, 2], "i": [0, 1, 2]})
    ).lazy()
    with pytest.raises((InvalidOperationError, NotImplementedError)):
        df.select(a=expr)
