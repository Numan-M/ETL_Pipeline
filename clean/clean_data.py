from extract.extract import turn_file_into_dataframe,col_names
import pandas as pd

def remove_columns_from_df(dataframe: pd.DataFrame, columns_to_drop: list) -> pd.DataFrame:
    dataframe.drop(columns_to_drop, axis=1, inplace=True)
    return dataframe


to_drop = [
    "customer_name",
    "card_number"
]

x = remove_columns_from_df(turn_file_into_dataframe("data/mockFile.csv",col_names), to_drop)
print(x)
