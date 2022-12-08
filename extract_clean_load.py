from pathlib import Path

import numpy as np
import pandas as pd
import pytest

col_names = [
    "timestamp",
    "store_name",
    "customer_name",
    "products",
    "total_price",
    "payment_method",
    "card_number",
]

file_path = "data/mockFile.csv"


def turn_file_into_dataframe(
    file_path: str, col_names: list = col_names
) -> pd.DataFrame:

    """Take a CSV file with the right number of columns and returns a data frame"""

    try:
        dataframe = pd.read_csv(Path(file_path), names=col_names)
        return dataframe
    except FileNotFoundError as e:
        print(f"There was no file at {file_path}")
        return e


to_drop = ["customer_name", "card_number"]


def remove_columns_from_df(
    dataframe: pd.DataFrame, columns_to_drop: list
) -> pd.DataFrame:
    """Removes specified columns from a dataframe"""

    dataframe.drop(columns_to_drop, axis=1, inplace=True)
    return dataframe


x = remove_columns_from_df(
    turn_file_into_dataframe("data/mockFile.csv", col_names), to_drop
)
print(x)
