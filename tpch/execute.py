from __future__ import annotations

import argparse
import os
from importlib import import_module

import dask.dataframe as dd
import pandas as pd
import polars as pl
import pyarrow as pa

import narwhals as nw

pd.options.mode.copy_on_write = True
pd.options.future.infer_string = True

DATA_DIR = "{}"
LINEITEM_PATH = os.path.join(DATA_DIR, "lineitem.parquet")  # noqa: PTH118
REGION_PATH = os.path.join(DATA_DIR, "region.parquet")  # noqa: PTH118
NATION_PATH = os.path.join(DATA_DIR, "nation.parquet")  # noqa: PTH118
SUPPLIER_PATH = os.path.join(DATA_DIR, "supplier.parquet")  # noqa: PTH118
PART_PATH = os.path.join(DATA_DIR, "part.parquet")  # noqa: PTH118
PARTSUPP_PATH = os.path.join(DATA_DIR, "partsupp.parquet")  # noqa: PTH118
ORDERS_PATH = os.path.join(DATA_DIR, "orders.parquet")  # noqa: PTH118
CUSTOMER_PATH = os.path.join(DATA_DIR, "customer.parquet")  # noqa: PTH118

BACKEND_NAMESPACE_KWARGS_MAP = {
    "pandas[pyarrow]": (pd, {"engine": "pyarrow", "dtype_backend": "pyarrow"}),
    "polars[lazy]": (pl, {}),
    "pyarrow": (pa, {}),
    "dask": (dd, {"engine": "pyarrow", "dtype_backend": "pyarrow"}),
}

BACKEND_COLLECT_FUNC_MAP = {
    "polars[lazy]": lambda x: x.collect(),
    "dask": lambda x: x.compute(),
}

QUERY_DATA_PATH_MAP = {
    "q1": (LINEITEM_PATH,),
    "q2": (REGION_PATH, NATION_PATH, SUPPLIER_PATH, PART_PATH, PARTSUPP_PATH),
    "q3": (CUSTOMER_PATH, LINEITEM_PATH, ORDERS_PATH),
    "q4": (LINEITEM_PATH, ORDERS_PATH),
    "q5": (
        REGION_PATH,
        NATION_PATH,
        CUSTOMER_PATH,
        LINEITEM_PATH,
        ORDERS_PATH,
        SUPPLIER_PATH,
    ),
    "q6": (LINEITEM_PATH,),
    "q7": (NATION_PATH, CUSTOMER_PATH, LINEITEM_PATH, ORDERS_PATH, SUPPLIER_PATH),
    "q8": (
        PART_PATH,
        SUPPLIER_PATH,
        LINEITEM_PATH,
        ORDERS_PATH,
        CUSTOMER_PATH,
        NATION_PATH,
        REGION_PATH,
    ),
    "q9": (
        PART_PATH,
        PARTSUPP_PATH,
        NATION_PATH,
        LINEITEM_PATH,
        ORDERS_PATH,
        SUPPLIER_PATH,
    ),
    "q10": (CUSTOMER_PATH, NATION_PATH, LINEITEM_PATH, ORDERS_PATH),
    "q11": (NATION_PATH, PARTSUPP_PATH, SUPPLIER_PATH),
    "q12": (LINEITEM_PATH, ORDERS_PATH),
    "q13": (CUSTOMER_PATH, ORDERS_PATH),
    "q14": (LINEITEM_PATH, PART_PATH),
    "q15": (LINEITEM_PATH, SUPPLIER_PATH),
    "q16": (PART_PATH, PARTSUPP_PATH, SUPPLIER_PATH),
    "q17": (LINEITEM_PATH, PART_PATH),
    "q18": (CUSTOMER_PATH, LINEITEM_PATH, ORDERS_PATH),
    "q19": (LINEITEM_PATH, PART_PATH),
    "q20": (PART_PATH, PARTSUPP_PATH, NATION_PATH, LINEITEM_PATH, SUPPLIER_PATH),
    "q21": (LINEITEM_PATH, NATION_PATH, ORDERS_PATH, SUPPLIER_PATH),
    "q22": (CUSTOMER_PATH, ORDERS_PATH),
}


def execute_query(query_id: str, data_dir: str, *, verbose: bool) -> None:
    query_module = import_module(f"tpch.queries.{query_id}")
    data_paths = [x.format(data_dir) for x in QUERY_DATA_PATH_MAP[query_id]]

    for backend, (native_namespace, kwargs) in BACKEND_NAMESPACE_KWARGS_MAP.items():
        print(f"\nRunning {query_id} with {backend=}")  # noqa: T201
        result = query_module.query(
            *(
                nw.scan_parquet(path, native_namespace=native_namespace, **kwargs)
                for path in data_paths
            )
        )
        if collect_func := BACKEND_COLLECT_FUNC_MAP.get(backend):
            result = collect_func(result)
        if verbose:
            print(result)  # noqa: T201


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute a TPCH query by number.")
    parser.add_argument("query", type=str, help="The query to execute, e.g. 'q1'.")
    parser.add_argument(
        "--data_dir",
        type=str,
        required=False,
        default="data",
        help="Where the generated data is stored",
    )
    parser.add_argument("--verbose", action="store_true", help="Whether to print results")
    args = parser.parse_args()

    execute_query(query_id=args.query, data_dir=args.data_dir, verbose=args.verbose)


if __name__ == "__main__":
    main()
