import dask.dataframe as dd
import pandas as pd
import logging
import re
from datetime import datetime
from typing import List


def read_csv_file(file_path: str) -> dd.DataFrame:
    """Read a CSV file using Dask and return a Dask DataFrame.

    Args:
        file_path (str): The path to the CSV file.

    Returns:
        dd.DataFrame: A Dask DataFrame.
    """
    df = dd.read_csv(file_path)
    return df


def remove_m3_unit(column_name: str) -> str:
    """Remove the 'm3' unit of measure from the fuel name.

    Args:
        column_name (str): The name of the fuel.

    Returns:
        str: The fuel name with the unit removed.
    """
    return re.sub(r"\s*\(.*\)", "", column_name)


def transform(
    df: dd.DataFrame, remove_values: bool, id_vars: List[str], months
) -> dd.DataFrame:
    """Transform the data in the Dask DataFrame.

    Args:
        df (dd.DataFrame): The input Dask DataFrame.
        remove_values (bool): Whether to remove specific fuel values.
        id_vars (List[str]): The list of ID columns.

    Returns:
        dd.DataFrame: The transformed Dask DataFrame.
    """
    df = df.drop(["TOTAL", "REGIÃO"], axis=1)
    df = df.melt(id_vars=id_vars, var_name="MONTH", value_name="volume")
    df["MONTH"] = df["MONTH"].map(months)
    df["year_month"] = df["ANO"].astype(str) + "-" + df["MONTH"]
    df["product"] = df["COMBUSTÍVEL"].apply(
        remove_m3_unit, meta=("COMBUSTÍVEL", "object")
    )
    df["unit"] = "m3"
    df = df.rename(columns={"ESTADO": "uf"})
    df["created_at"] = datetime.now().strftime("%Y-%m-%d")

    df = df.astype(
        {
            "year_month": "string",
            "uf": "string",
            "product": "string",
            "unit": "string",
            "volume": "double",
            "created_at": "datetime64[ns]",
        }
    )

    if remove_values:
        df = df[~df["product"].isin(remove_values)]

    return df[["year_month", "uf", "product", "unit", "volume", "created_at"]]


def structure_dim_tables(
    file_path: str, output_path: str, remove_values: bool, id_vars: List[str], months
) -> dd.DataFrame:
    """Structure the data and save it in Parquet format.

    Args:
        file_path (str): The path to the input CSV file.
        output_path (str): The path for the output Parquet file.
        remove_values (bool): Whether to remove specific fuel values.
        id_vars (List[str]): The list of ID columns.

    Returns:
        dd.DataFrame: The structured Dask DataFrame.
    """
    df = read_csv_file(file_path)
    df = transform(df, remove_values, id_vars, months)
    df = df.compute()
    df.to_parquet(output_path, index=False)
    logging.info(f"Data loaded at: {output_path}")
    return df
