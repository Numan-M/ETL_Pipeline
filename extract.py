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


def turn_file_into_dataframe(file_path: str, col_names: list = col_names) -> pd.DataFrame :
    try:
        dataframe = pd.read_csv(Path(file_path), names=col_names)
        dataframe.head()
        return dataframe
    except FileNotFoundError as e:
        print(f"There was no file at {file_path}")
        #print(e)
        return e
        
    

print((turn_file_into_dataframe("data/mockFileWithMissingHeaders.csv", col_names)))
print((type(turn_file_into_dataframe("data/mockFileWithMissingHeaders.csv", col_names))) == pd.DataFrame)



