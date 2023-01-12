from pathlib import Path

import boto3
import numpy as np
import pandas as pd

# import psycopg2
# import psycopg2.extras as extras
# from connect_to_db import connecting_to_db


bucket_name = "stackbucketg5"
s3_client = boto3.client("s3", endpoint_url="https://s3.eu-west-1.amazonaws.com")


def turn_file_into_dataframe(file_path: str, col_names: list) -> pd.DataFrame:

    """Take a CSV file with the right number of columns and returns a data frame"""

    try:
        dataframe = pd.read_csv(Path(file_path), names=col_names)
        return dataframe
    except FileNotFoundError as e:
        print(f"There was no file at {file_path}")
        return e


def format_timestamps(df: pd.DataFrame) -> pd.DataFrame:

    df["timestamp"] = pd.to_datetime(
        df.timestamp
    )  # this fills the column with timestamp object which did not work in the query
    df["timestamp"] = df["timestamp"].astype(str)

    return df


def remove_columns_from_df(
    dataframe: pd.DataFrame, columns_to_drop: list
) -> pd.DataFrame:

    """Removes specified columns from a dataframe"""

    try:
        dataframe.drop(columns_to_drop, axis=1, inplace=True)
        dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"])
        return dataframe
    except:
        print("dataframe does not contain specified columns")


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


def foreign_key_dict(df, col_name):
    """This Function is creating a foreign key condition using exisitng column"""
    increment = 1
    foreign_key_dict = {}
    for item in df[f"{col_name}"].unique():
        foreign_key_dict.update({f"{item}": increment})
        increment += 1
    return foreign_key_dict


def foreign_key_cols(dict, df, new_col, exist_col):
    """This function is creating foreign key column"""
    condition = {}
    condition.update(dict)
    df[new_col] = df[exist_col].map(condition)
    return df


def collect_names_of_files_in_bucket(bucket_name: str) -> list:
    response = s3_client.list_objects_v2(Bucket=bucket_name)
    content_list = response["Contents"]
    object_key_list = []
    for s3_object in content_list:
        object_key_list.append(s3_object["Key"])
    print(f"You have collected these files: {object_key_list}")
    return object_key_list


col_names = [
    "timestamp",
    "store_name",
    "customer_name",
    "products",
    "total_price",
    "payment_method",
    "card_number",
]

file_path = "../data/chesterfield_25-08-2021_09-00-00.csv"

to_drop = ["customer_name", "card_number"]

# Turning csv into df
dataframe_file = turn_file_into_dataframe(file_path, col_names)

# Dropping privacy columns
clean_df = remove_columns_from_df(dataframe_file, to_drop)

# Changing format of timestamp
clean_df = format_timestamps(clean_df)

# Splitting products into individual columns
clean_split_df = splitting_products_column(clean_df)

# 1. Creating a payment method df
payment_methods_table = pd.DataFrame(
    clean_split_df["payment_method"].unique(), columns=["payment_method"]
)

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
store_FK = foreign_key_dict(clean_df, "store_name")
payment_method_FK = foreign_key_dict(clean_df, "payment_method")
customer_basket_table = pd.DataFrame(
    clean_df, columns=["timestamp", "store_name", "total_price", "payment_method"]
)
customer_basket_table = foreign_key_cols(
    store_FK, customer_basket_table, "store_id", "store_name"
)
customer_basket_table = foreign_key_cols(
    payment_method_FK, customer_basket_table, "payment_method_id", "payment_method"
)
customer_basket_table.drop(["store_name", "payment_method"], axis=1, inplace=True)
customer_basket_table_cols = [
    "store_id",
    "payment_method_id",
    "timestamp",
    "total_price",
]
customer_basket_table = customer_basket_table[customer_basket_table_cols]

# 5. Creating a sales df
product_id_FK_condition = foreign_key_dict(clean_split_df, "product_name")
sales_table = pd.DataFrame(clean_split_df, columns=["product_name"])
sales_table = foreign_key_cols(
    product_id_FK_condition, sales_table, "product_id", "product_name"
)
sales_table["customer_basket_id"] = sales_table.index + 1
sales_table.drop(["product_name"], axis=1, inplace=True)
sales_table = sales_table[["customer_basket_id", "product_id"]]
