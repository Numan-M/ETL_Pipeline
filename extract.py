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


def turn_file_into_dataframe(file_path: str, col_names: list) -> pd.DataFrame:

    df = pd.read_csv(Path(file_path), names=col_names)
    df.head()
    return df


turn_file_into_dataframe(file_path, col_names)


# happy path

# Reads file with headers, returns a frame with headers


# Reads file with no headers, returns a frame with headers


# unhappy path

# Reads file, file is empty
