import connectorx as cx
import pandas as pd

conn_str = "postgresql://username:password@localhost:5432/mydatabase"


def fetch_data(
    conn_str: str,
    query: str,
    backend: str = "pandas",
    partition_on: str = None,
    partition_num: int = 1,
):
    kwargs = {}
    if partition_on:
        kwargs["partition_on"] = partition_on
        kwargs["partition_num"] = partition_num
    if backend not in ["pandas", "polars"]:
        raise ValueError("backend must be 'pandas' or 'polars'")
    return_type = backend

    results = cx.read_sql(conn_str, query, return_type=return_type, **kwargs)
    return results


# Usage:
pandas_df = fetch_data(conn_str, "SELECT * FROM products", backend="pandas")
polars_df = fetch_data(
    conn_str,
    "SELECT * FROM products",
    backend="polars",
    partition_on="id",
    partition_num=4,
)
