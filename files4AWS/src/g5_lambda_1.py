from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import boto3
import numpy as np
import pandas as pd

from connecting import connection

bucket_name = "delon8-group5"
s3_client = boto3.client("s3", endpoint_url="https://s3.eu-west-1.amazonaws.com")


def remove_columns_from_df(
    dataframe: pd.DataFrame, columns_to_drop: list
) -> pd.DataFrame:
    print("DHFDHJKFJDFHJDFHD remove_columns_from_df")

    """Removes specified columns from a dataframe"""

    try:
        dataframe.drop(columns_to_drop, axis=1, inplace=True)
        dataframe["timestamp"] = pd.to_datetime(dataframe["timestamp"])
        return dataframe
    except Exception as e:
        print(e)
        print("dataframe does not contain specified columns")


def splitting_products_column(df):
    print("DHFDHJKFJDFHJDFHD splitting_products_column")

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
    print("DHFDHJKFJDFHJDFHD foreign_key_dict")
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


##ADDED BY ARAM


def turn_file_into_dataframe(bucket_name, object_key, col_names) -> pd.DataFrame:

    """Take a CSV file with the right number of columns and returns a data frame"""

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        df_s3_data = pd.read_csv(response["Body"], sep=",", names=col_names)
        return df_s3_data
    except FileNotFoundError as e:
        print(f"There was no file at {file_path}")
        return e


yesterday = datetime.now() - timedelta(1)
yesterday = str(yesterday)[0:10]
yesterday = yesterday.replace("-", "/")
yesterday = yesterday.replace("/0", "/")


def collect_names_of_files_in_bucket(bucket_name: str) -> list:
    print("DHFDHJKFJDFHJDFHD collect_names_of_files_in_bucket")

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=yesterday)
    content_list = response["Contents"]
    object_key_list = []
    for s3_object in content_list:
        object_key_list.append(s3_object["Key"])
    print(f"You have collected these files: {object_key_list}")
    return object_key_list


def get_recently_logged_files(connection):
    cursor = connection.cursor()
    query_to_get_files_from_last_minute = "SELECT * FROM  public.file_log WHERE time_logged >= GETDATE() - '20 minute'::INTERVAL;"
    cursor.execute(query_to_get_files_from_last_minute)
    result = cursor.fetchall()
    list_of_file_processed_in_last_20min = []
    for row in result:
        list_of_file_processed_in_last_20min.append(row[0])
    return list_of_file_processed_in_last_20min


def check_if_file_logged(object_key, connection):
    print("DHFDHJKFJDFHJDFHD check_if_file_logged")
    try:
        recently_logged_files = get_recently_logged_files(connection)
        return object_key in recently_logged_files
    except Exception as error:
        print(error)


def log_filename(object_key, connection_to_db):
    query_to_insert_file_name = f"INSERT INTO public.file_log(file_name, time_logged) VALUES ('{object_key}', current_timestamp);"
    cursor = connection_to_db.cursor()
    cursor.execute(query_to_insert_file_name)
    connection_to_db.commit()
    print(f"File '{object_key}' has been processed")


##FINISH ADDED BY ARAM

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


# collect list of files from bucket
# list_of_file_names = collect_names_of_files_in_bucket(bucket_name)

# Check if the files in the list files are already logged


def process_list_of_files(list_of_file_names):
    for object_key in list_of_file_names:
        if check_if_file_logged(object_key, connection) == False:

            # Turning csv into df
            dataframe_file = turn_file_into_dataframe(
                bucket_name, object_key, col_names
            )

            # Dropping privacy columns
            clean_df = remove_columns_from_df(dataframe_file, columns_to_drop)

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
                clean_df,
                columns=["timestamp", "store_name", "total_price", "payment_method"],
            )
            customer_basket_table = foreign_key_cols(
                store_FK, customer_basket_table, "store_id", "store_name"
            )
            customer_basket_table = foreign_key_cols(
                payment_method_FK,
                customer_basket_table,
                "payment_method_id",
                "payment_method",
            )
            customer_basket_table.drop(
                ["store_name", "payment_method"], axis=1, inplace=True
            )
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
            sales_table = sales_table.convert_dtypes()

            return {
                "customer_basket_table": customer_basket_table,
                "payment_methods_table": payment_methods_table,
                "products_table": products_table,
                "sales_table": sales_table,
                "store_name_table": store_name_table,
            }
