from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from narwhals.expr import Expr

ExprT = TypeVar("ExprT", bound="Expr")


class ExprCatNamespace(Generic[ExprT]):
    def __init__(self, expr: ExprT) -> None:
        self._expr = expr

    def get_categories(self) -> ExprT:
        """Get unique categories from column.

        Examples:
            >>> import polars as pl
            >>> import narwhals as nw
            >>> df_native = pl.DataFrame(
            ...     {"fruits": ["apple", "mango", "mango"]},
            ...     schema={"fruits": pl.Categorical},
            ... )
            >>> df = nw.from_native(df_native)
            >>> df.select(nw.col("fruits").cat.get_categories()).to_native()
            shape: (2, 1)
            ┌────────┐
            │ fruits │
            │ ---    │
            │ str    │
            ╞════════╡
            │ apple  │
            │ mango  │
            └────────┘
        """
        # Create tree node for cat method call
        tree_node = None
        if self._expr._tree is not None:
            from narwhals._expression_tree import NamespaceMethodCallNode

            tree_node = NamespaceMethodCallNode(
                self._expr._tree, "cat", "get_categories", (), {}
            )

        return self._expr._with_elementwise(
            lambda plx: self._expr._to_compliant_expr(plx).cat.get_categories(),
            tree_node=tree_node,
        )
