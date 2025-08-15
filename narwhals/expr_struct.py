from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from narwhals.expr import Expr

ExprT = TypeVar("ExprT", bound="Expr")


class ExprStructNamespace(Generic[ExprT]):
    def __init__(self, expr: ExprT) -> None:
        self._expr = expr

    def _create_namespace_tree_node(
        self, method_name: str, *args: Any, **kwargs: Any
    ) -> Any:
        """Helper to create namespace tree nodes."""
        return self._expr._create_namespace_tree_node(
            "struct", method_name, *args, **kwargs
        )

    def field(self, name: str) -> ExprT:
        r"""Retrieve a Struct field as a new expression.

        Arguments:
            name: Name of the struct field to retrieve.

        Examples:
            >>> import polars as pl
            >>> import narwhals as nw
            >>> df_native = pl.DataFrame(
            ...     {"user": [{"id": "0", "name": "john"}, {"id": "1", "name": "jane"}]}
            ... )
            >>> df = nw.from_native(df_native)
            >>> df.with_columns(name=nw.col("user").struct.field("name"))
            ┌───────────────────────┐
            |  Narwhals DataFrame   |
            |-----------------------|
            |shape: (2, 2)          |
            |┌──────────────┬──────┐|
            |│ user         ┆ name │|
            |│ ---          ┆ ---  │|
            |│ struct[2]    ┆ str  │|
            |╞══════════════╪══════╡|
            |│ {"0","john"} ┆ john │|
            |│ {"1","jane"} ┆ jane │|
            |└──────────────┴──────┘|
            └───────────────────────┘
        """
        tree_node = self._create_namespace_tree_node("field", name)
        return self._expr._with_elementwise(
            lambda plx: self._expr._to_compliant_expr(plx).struct.field(name),
            tree_node=tree_node,
        )
