from pathlib import Path

import collections
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


class HeadersWrongError(Exception):
    pass


def check_all_headers_present(
    file_path: str, expected_col_names: list = col_names
) -> bool:

    """This function takes in a file through file_path and expected column headers and outputs
    a Boolean True if all expected headers are present and are in the right order,
    or False if there are missing headers"""

    try:
        dataframe = pd.read_csv(Path(file_path))
        actual_column_names = list(dataframe.columns)
        return actual_column_names == expected_col_names

    except FileNotFoundError as error:
        print(f"There was no file at {file_path}")
        # print(e)
        return error


def turn_file_into_dataframe(
    file_path: str, col_names: list = col_names
) -> pd.DataFrame:

    """This function takes in a file through a file_path and column headers that default to
    a value, in case the file has no headers and returns a dataframe with the headers provided
    or the default headers"""

    try:
        if check_all_headers_present(file_path, col_names) or (len(col_names) != :
            dataframe = pd.read_csv(Path(file_path), names=col_names)
            return dataframe
        else:
            raise HeadersWrongError(
                f"The file at {file_path} is has the wrong headers (some may be missing, have the wrong name, or be in the wrong order"
            )

    except FileNotFoundError as error:
        print(f"There was no file at {file_path}")
        # print(e)
        return error


#print((check_all_headers_present("data/mockFileWithMissingHeaders.csv", col_names)))
# print((check_all_headers_present("data/mockFileWithHeaders.csv", col_names)))


print((turn_file_into_dataframe("data/mockFile.csv", col_names)))
# print((type(turn_file_into_dataframe("data/mockFileWithMissingHeaders.csv", col_names))) == pd.DataFrame)
