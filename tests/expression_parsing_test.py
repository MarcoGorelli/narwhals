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
    ],
)
def test_over_invalid(constructor: Constructor, expr: nw.Expr) -> None:
    df = nw.from_native(constructor({"a": [-1, 2, 3], "i": [0, 1, 2]})).lazy()
    with pytest.raises(InvalidOperationError):
        df.select(a=expr)


def test_misleading_order_by() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().over(order_by="b")


def test_double_over() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().over("b").over("c")


def test_double_agg() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().mean()
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().sum()


def test_filter_aggregation() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().drop_nulls()


def test_rank_aggregation() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().rank()
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().is_unique()


def test_diff_aggregation() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().diff()


def test_invalid_over() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").fill_null(3).over("b")


def test_nested_over() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().over("b").over("c")
    with pytest.raises(InvalidOperationError):
        nw.col("a").mean().over("b").over("c", order_by="i")


def test_filtration_over() -> None:
    with pytest.raises(InvalidOperationError):
        nw.col("a").drop_nulls().over("b")
    with pytest.raises(InvalidOperationError):
        nw.col("a").drop_nulls().over("b", order_by="i")
    with pytest.raises(InvalidOperationError):
        nw.col("a").diff().drop_nulls().over("b", order_by="i")
