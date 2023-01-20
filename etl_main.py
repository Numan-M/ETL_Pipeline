import pandas as pd
import psycopg2

from src.connecting import connecting_to_db
from src.creating_tables import query_payment
from src.creating_tables import query_products
from src.creating_tables import query_sales
from src.creating_tables import query_store
from src.creating_tables import query_transactions
from src.g5_lambda_1 import payment_methods_table
from src.g5_lambda_1 import products_table
from src.g5_lambda_1 import sales_table
from src.g5_lambda_1 import store_name_table
from src.g5_lambda_1 import transactions_table


def main():

    cursor = connecting_to_db.cursor()
    run_query(cursor, query_store)
    run_query(cursor, query_payment)
    run_query(cursor, query_products)
    run_query(cursor, query_transactions)
    run_query(cursor, query_sales)
    insert_values_in_table(cursor, store_name_table, "stores")
    insert_values_in_table(cursor, payment_methods_table, "payment_methods")
    insert_values_in_table(cursor, products_table, "products")
    # insert_values_in_table(cursor,staging_table_store_id,"staging_store_id")
    # insert_values_in_table(cursor,staging_table_payment_id,"staging_payment_method_id")
    cursor.execute(f"SELECT COUNT(*) FROM transactions")
    count = cursor.fetchone()[0]
    count_variable = int(count)
    sales_table["transaction_id"] += count_variable
    # insert_values_in_table(cursor, sales_table, "sales")
    load_transactions_to_db(cursor, transactions_table)
    load_sales_to_db(cursor, sales_table)
    cursor.close()
    connecting_to_db.close()


def run_query(cursor, query):
    try:

        cursor.execute(query)
        connecting_to_db.commit()
        print(f"Successfully ran query")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Following query failed {query}")
        print(error)


def insert_values_in_table(cursor, df: pd.DataFrame, table_name: str):
    cols = ", ".join(df.columns)
    asterisk = ", ".join(["%s"] * len(df.columns))
    query = (
        f"INSERT INTO {table_name}({cols}) VALUES ({asterisk}) ON CONFLICT DO NOTHING"
    )
    for index, row in df.iterrows():
        values = tuple(row)
        cursor.execute(query, values)
    connecting_to_db.commit()
    return


def load_transactions_to_db(cursor, df):
    # create connection

    tuples = [
        tuple(x) for x in df.to_numpy()
    ]  # create a tuple for each row of data, ready to insert

    try:
        for x in tuples:
            cursor.execute(
                query=f"""INSERT INTO transactions(timestamp, store_id, total_price, payment_method_id) VALUES ('{x[0]}',
            (SELECT store_id FROM stores WHERE stores.store_name = '{x[1]}'), {x[2]},
            (SELECT payment_method_id FROM payment_methods WHERE payment_methods.payment_method = '{x[3]}'));"""
            )
        connecting_to_db.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return
    print("The data has been inserted")


def load_sales_to_db(cursor, df):
    # create connection

    tuples = [
        tuple(x) for x in df.to_numpy()
    ]  # create a tuple for each row of data, ready to insert

    try:
        for x in tuples:
            cursor.execute(
                query=f"""INSERT INTO sales(transaction_id,product_id) VALUES (
            (SELECT transaction_id FROM transactions WHERE transactions.transaction_id = '{x[0]}'),
            (SELECT product_id FROM products WHERE products.product_name = '{x[1]}'));"""
            )
        connecting_to_db.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        return
    print("The data has been inserted")


if __name__ == "__main__":
    main()
