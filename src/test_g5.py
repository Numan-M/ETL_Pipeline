from pathlib import Path

import pandas as pd
import psycopg2
import pytest

from src.connecting import connect_to_database
from src.g5_lambda_1 import remove_columns_from_df
from src.g5_lambda_1 import splitting_products_column
from src.g5_lambda_1 import turn_file_into_dataframe


def test_connect_to_database_success():
    # Test successful connection
    connection_details = {
        "database": "test",
        "user": "postgres",
        "password": "pass",
        "host": "localhost",
        "port": "5432",
    }
    conn = connect_to_database(connection_details)
    assert conn is not None
    assert conn.closed == 0
    conn.close()


def test_connect_to_database_failure():
    # Test failed connection
    connection_details_error = {
        "database": "invalid_db",
        "user": "postgres",
        "password": "pass",
        "host": "localhost",
        "port": "5432",
    }
    with pytest.raises(psycopg2.OperationalError):
        connect_to_database(connection_details_error)


file_path = "data/chesterfield_25-08-2021_09-00-00.csv"
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


def test_turn_file_into_dataframe():
    dataframe_file = turn_file_into_dataframe(file_path, col_names)
    assert isinstance(dataframe_file, pd.DataFrame)
    assert "timestamp" in dataframe_file.columns


def test_remove_columns_from_df():
    dataframe_file = turn_file_into_dataframe(file_path, col_names)
    clean_df = remove_columns_from_df(dataframe_file, columns_to_drop)
    assert "customer_name" not in clean_df.columns
    assert "card_number" not in clean_df.columns
    assert isinstance(clean_df["timestamp"][0], pd._libs.tslibs.timestamps.Timestamp)


def test_splitting_products_column():
    dataframe_file = turn_file_into_dataframe(file_path, col_names)
    clean_df = remove_columns_from_df(dataframe_file, columns_to_drop)
    clean_split_df = splitting_products_column(clean_df)
    assert "products" not in clean_split_df.columns
    assert "product_name" in clean_split_df.columns
    assert "price" in clean_split_df.columns
    assert "timestamp" in clean_split_df.columns
    assert "store_name" in clean_split_df.columns
    assert "total_price" in clean_split_df.columns
    assert "payment_method" in clean_split_df.columns
    assert isinstance(clean_split_df, pd.DataFrame)


def test_turn_file_into_dataframe_exception():
    result = turn_file_into_dataframe("data/invalid_path.csv", col_names)
    assert isinstance(result, FileNotFoundError)


def test_remove_columns_from_df_exception():
    dataframe_file = turn_file_into_dataframe(file_path, col_names)
    clean_df = remove_columns_from_df(dataframe_file, ["invalid_column"])
    assert "invalid_column" not in clean_df.columns


def test_splitting_products_column_exception():
    dataframe_file = turn_file_into_dataframe(file_path, col_names)
    clean_df = remove_columns_from_df(dataframe_file, columns_to_drop)
    clean_df = clean_df.drop(["products"], axis=1)
    with pytest.raises(KeyError):
        splitting_products_column(clean_df)
