from pathlib import Path

import numpy as np
import pandas as pd

# from connecting import connecting_to_db
# cursor = connecting_to_db.cursor()
file_path = "data/London_25-08-2021_09-00-00.csv"


def turn_file_into_dataframe(file_path: str, col_names: list) -> pd.DataFrame:

    """Take a CSV file with the right number of columns and returns a data frame"""

    try:
        dataframe = pd.read_csv(Path(file_path), names=col_names)
        return dataframe
    except FileNotFoundError as e:
        print(f"There was no file at {file_path}")
        return e


def remove_columns_from_df(
    dataframe: pd.DataFrame, columns_columns_to_drop: list
) -> pd.DataFrame:
    try:
        dataframe.drop(columns_columns_to_drop, axis=1, inplace=True)
        dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"])
    except:
        print("dataframe does not contain specified columns")
    return dataframe


def splitting_products_column(df):

    """Cleansing products column"""

    col = "products"
    # Turn products into a list
    df[col] = df[col].str.split(", ")

    # Spliting the products into individual row
    df = df.explode(col)

    # Splitting from the last ' - '
    df[col] = df[col].str.rsplit(" - ", n=1)

    # Two new columns
    df[["product_name", "price"]] = pd.DataFrame(df.products.tolist(), index=df.index)

    # drop old columns
    df = df.drop(columns=[col])

    # Rearrange columns
    new_col = [
        "timestamp",
        "store_name",
        "product_name",
        "price",
        "total_price",
        "payment_method",
    ]
    df = df[new_col]
    return df


col_names = [
    "timestamp",
    "store_name",
    "customer_name",
    "products",
    "total_price",
    "payment_method",
    "card_number",
]


columns_to_drop = ["customer_name", "card_number"]

# Turning csv into df
dataframe_file = turn_file_into_dataframe(file_path, col_names)

# Dropping privacy columns
clean_df = remove_columns_from_df(dataframe_file, columns_to_drop)

# Splitting products into individual columns
clean_split_df = splitting_products_column(clean_df)

# 1. Creating a payment method df
payment_methods_table = pd.DataFrame(
    clean_split_df["payment_method"].unique(), columns=["payment_method"]
)
payment_methods_table = payment_methods_table.assign(
    payment_method_id=payment_methods_table.index
)
payment_methods_table = payment_methods_table[["payment_method_id", "payment_method"]]


# # 2. Creating a store name df
store_name_table = pd.DataFrame(
    clean_split_df["store_name"].unique(), columns=["store_name"]
)

# 3. Creating a product df
products_table = pd.DataFrame(
    clean_split_df[["product_name", "price"]].drop_duplicates(),
    columns=["product_name", "price"],
)


# 4. Creating a customer_basket_table

customer_basket_table = pd.DataFrame(
    clean_df, columns=["timestamp", "store_name", "total_price", "payment_method"]
)
# customer_basket_table = customer_basket_table.rename(columns={'store_name':'store_id','payment_method':'payment_method_id'})
# 5.
def retrieve_col_length(cursor, table):
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    count_variable = int(count)
    return count_variable


sales_table = pd.DataFrame(
    clean_split_df, columns=["customer_basket_id", "product_name"]
)
sales_table["customer_basket_id"] = sales_table.index + 1
# sales_table = sales_table.rename(columns={'product_name':'product_id'})
# print(sales_table)
