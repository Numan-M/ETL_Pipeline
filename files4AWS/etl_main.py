import pandas as pd
import psycopg2
import psycopg2.extras as extras

from src.connecting import connecting_to_db
from src.creating_tables import alter_transaction_paymentid
from src.creating_tables import alter_transaction_storeid
from src.creating_tables import query_basket
from src.creating_tables import query_products
from src.creating_tables import query_store
from src.creating_tables import script_payment
from src.creating_tables import transaction
from src.g5_lambda_1 import basket_table
from src.g5_lambda_1 import payment_methods_table
from src.g5_lambda_1 import products_table
from src.g5_lambda_1 import store_table
from src.g5_lambda_1 import transaction_table


def main():
    create_table(connecting_to_db, query_store)
    create_table(connecting_to_db, script_payment)
    create_table(connecting_to_db, query_products)
    create_table(connecting_to_db, query_basket)
    create_table(connecting_to_db, transaction)
    alter_table(connecting_to_db, alter_transaction_storeid)
    alter_table(connecting_to_db, alter_transaction_paymentid)

    insert_values_in_table(connecting_to_db, store_table, "stores")
    insert_values_in_table(connecting_to_db, payment_methods_table, "payment_methods")
    insert_values_in_table(connecting_to_db, products_table, "products")
    insert_values_in_table(connecting_to_db, basket_table, "basket")
    insert_values_in_table(connecting_to_db, transaction_table, "transaction")


def create_table(conn, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        print("Query for table creation executed")
    except Exception as e:
        print(e)
        print("table not made")


def alter_table(conn, alter_query):
    try:

        cursor = conn.cursor()

        cursor.execute(alter_query)
        print("tables alter")
    except:
        print("no tables alter")


def insert_values_in_table(connection, df, table_used):
    """Loading data into DB"""

    tuples = [tuple(x) for x in df.to_numpy()]

    columns = ",".join(list(df.columns))

    query = f"INSERT INTO {table_used}({columns}) VALUES %s"
    """--> INSERT INTO payment_methods(payment_method) VALUES ('CARD'),..."""

    cursor = connection.cursor()
    try:
        extras.execute_values(cursor, query, tuples)

        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        print("the dataframe is inserted")


if __name__ == "__main__":
    main()
