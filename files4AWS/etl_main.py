import pandas as pd
import redshift_connector

from src.connecting import connection
from src.creating_tables import alter_customer_basket_paymentid
from src.creating_tables import alter_customer_basket_storeid
from src.creating_tables import alter_sales_customer_basket_id
from src.creating_tables import alter_sales_product_id
from src.creating_tables import query_customer_basket
from src.creating_tables import query_payment
from src.creating_tables import query_products
from src.creating_tables import query_sales
from src.creating_tables import query_store
from src.g5_lambda_1 import customer_basket_table
from src.g5_lambda_1 import payment_methods_table
from src.g5_lambda_1 import products_table
from src.g5_lambda_1 import sales_table
from src.g5_lambda_1 import store_name_table


def main():
    run_query(connection, query_store)
    run_query(connection, query_payment)
    run_query(connection, query_products)
    run_query(connection, query_customer_basket)
    run_query(connection, query_sales)

    run_query(connection, alter_customer_basket_storeid)
    run_query(connection, alter_customer_basket_paymentid)

    run_query(connection, alter_sales_customer_basket_id)
    run_query(connection, alter_sales_product_id)

    insert_values_in_table(connection, store_name_table, "stores")
    insert_values_in_table(connection, payment_methods_table, "payment_methods")
    insert_values_in_table(connection, products_table, "products")
    insert_values_in_table(connection, customer_basket_table, "customer_basket")
    insert_values_in_table(connection, sales_table, "sales")


def run_query(conn, query):
    try:

        cursor = conn.cursor()
        cursor.execute(query)
        print(f"Successfully run query: {query} ")

    except (Exception, redshift_connector.DatabaseError) as error:
        print(f"Following query failed {query}")
        print(error)


def insert_values_in_table(connection, df, table_used):
    """Loading data into DB"""

    tuples = [tuple(x) for x in df.to_numpy()]
    values_to_enter = str(tuples).strip("[]").replace(",)", ")")

    columns = ",".join(list(df.columns))

    query = f"INSERT INTO public.{table_used} ({columns}) VALUES {values_to_enter};"
    """--> INSERT INTO payment_methods(payment_method) VALUES ('CARD'),..."""

    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("the dataframe is inserted")

    except (Exception, redshift_connector.DatabaseError) as error:
        print(error)

    finally:
        connection.close()


if __name__ == "__main__":
    main()
