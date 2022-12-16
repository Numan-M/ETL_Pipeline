from pathlib import Path
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import numpy as np
from connect_to_db import connecting_to_db
from csv_into_df import column_names, to_drop_stores,to_drop_payment,to_drop_transaction
from csv_into_df import df_products
from creating_tables import script_store,script_payment,query_transaction,alter_t,query_products
def main():



    file_path = "data/chesterfield_25-08-2021_09-00-00 - Copy.csv"


    """Creating dataframes for each table"""
    df = turn_file_into_dataframe(file_path,column_names)
    df_payment= turn_file_into_dataframe(file_path,column_names)
    df_transaction = turn_file_into_dataframe(file_path,column_names)
    
 
    """Cleaning & transforming dataframes"""
    clean_df_store1 = remove_columns_from_df(df,to_drop_stores)
    clean_df_store = clean_df_store1.drop_duplicates(inplace=False)
    clean_df_payment1 = remove_columns_from_df(df_payment,to_drop_payment)
    clean_df_payment = clean_df_payment1.drop_duplicates(inplace=False)
    clean_df_transaction = remove_columns_from_df(df_transaction,to_drop_transaction)
    clean_df_products = df_products
    drop_p=["products","total_price","payment_method"]
    clean_df_products.drop(drop_p, axis=1, inplace=True)
    
   
    """Creating tables in DB"""
    create_table(connecting_to_db,script_store)
    create_table(connecting_to_db,script_payment)
    create_table(connecting_to_db,query_transaction)
    create_table(connecting_to_db,query_products)


    """Loading data into DB"""
    insert_values_in_table(connecting_to_db,clean_df_store,'stores')
    insert_values_in_table(connecting_to_db,clean_df_payment,'payment_methods')
    insert_values_in_table(connecting_to_db,clean_df_transaction,'transactions')
    insert_values_in_table(connecting_to_db,clean_df_products,'products')

    """altering tables in DB"""
    alter_table(connecting_to_db,alter_t)
    
   


def turn_file_into_dataframe(file_path, col_names) -> pd.DataFrame:

    """Take a CSV file with the right number of columns and returns a data frame"""

    try:
        dataframe = pd.read_csv(Path(file_path), names=col_names)
        print("succesfully created Dataframe")
        return dataframe
        
    except FileNotFoundError as e:
        print(f"There was no file at {file_path}")
        return e


def remove_columns_from_df(dataframe, columns_to_drop) -> pd.DataFrame:

    """Removes specified columns from a dataframe"""
    try:
        dataframe['timestamp'] = pd.to_datetime(dataframe['timestamp'])
        dataframe.drop(columns_to_drop, axis=1, inplace=True)
        
        return dataframe
    except:
        print("dataframe does not contain specified columns")


def create_table(conn,query):
    try:
        cursor = conn.cursor()
    
        cursor.execute(query)
        print("table made")
        conn.commit()
    except:
        print("not made")

def alter_table(conn,alter):
    try:
        cursor = conn.cursor()
    
        cursor.execute(alter)
        print("table altered")
        conn.commit()
    except:
        print("not altered")



def insert_values_in_table(conn, df, table_used):
    """Loading data into DB"""
    
    tuples = [tuple(x) for x in df.to_numpy()]
    


    columns = ','.join(list(df.columns))
    
    query = "INSERT INTO %s(%s) VALUES %%s" % (table_used, columns)
    """--> INSERT INTO payment_methods(payment_method) VALUES ('CARD'),..."""
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        print("the dataframe is inserted")



if __name__ == "__main__":
    """
    this runs the main function 
    """
    main() 

