from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import psycopg2
import psycopg2.extras as extras
from connect_to_db import connecting_to_db
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
    try:
        dataframe.drop(columns_to_drop, axis=1, inplace=True)
        dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
        return dataframe
    except:
        print("dataframe does not contain specified columns")


clean_dataframe = remove_columns_from_df(turn_file_into_dataframe(file_path,col_names),to_drop)

query =  ('''
        CREATE TABLE IF NOT EXISTS orders(
        order_id SERIAL PRIMARY KEY not null,
        timestamp date not null,
        store_name text not null,
        products text not null,
        total_price float not null,
        payment_method text not null
        
        );
        ''')

def create_table(conn,query) ->psycopg2.extensions.connection:
    cursor = conn.cursor()
    cursor.execute(query)

def insert_values_in_table(conn, df, table_used) -> psycopg2.extensions.cursor:
    """
    function below takes three arguemnts , connnection, dataframe and table name inside connection.
    tuples--> creating a list of tuples from the dataframe. Has to be pass in values inside database
    cols --> columns are comma-separated
    query --> inserting
    """

    tuples = [tuple(x) for x in df.to_numpy()]


    columns = ','.join(list(df.columns))
    
    query = "INSERT INTO %s(%s) VALUES %%s" % (table_used, columns)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        print("the dataframe is inserted")

insert_values_in_table(connecting_to_db, clean_dataframe, 'orders')
connecting_to_db.commit()
connecting_to_db.close()