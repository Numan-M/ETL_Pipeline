import pandas as pd
import psycopg2
import psycopg2.extras as extras

from src.connecting import connect_to_database
from src.connecting import connection_details
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
    connecting_to_db = connect_to_database(connection_details)
    cursor = connecting_to_db.cursor()
    run_query(cursor, query_store)
    run_query(cursor, query_payment)
    run_query(cursor, query_products)
    run_query(cursor, query_transactions)
    run_query(cursor, query_sales)
    insert_values_in_table(cursor, store_name_table, "stores")
    #### CHANGE VAR NAME
    cursor.execute(f"SELECT COUNT(*) FROM payment_methods")
    count1 = cursor.fetchone()[0]
    count_variable1 = int(count1)
    payment_methods_table["payment_method_id"] += count_variable1
    insert_values_in_table(cursor, payment_methods_table, "payment_methods")

    insert_values_in_table(cursor, products_table, "products")

    cursor.execute(f"SELECT COUNT(*) FROM transactions")
    count = cursor.fetchone()[0]
    count_variable = int(count)
    sales_table["transaction_id"] += count_variable
    insert_values_in_table(cursor, sales_table, "sales")
    insert_values_in_table(cursor, transactions_table, "transactions")
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


# def insert_values_in_table(connection, df, table_used):
#     """Loading data into DB"""

#     tuples = [tuple(x) for x in df.to_numpy()]

#     columns = ",".join(list(df.columns))

#     query = f"INSERT INTO {table_used}({columns}) VALUES %s"
#     """--> INSERT INTO payment_methods(payment_method) VALUES ('CARD'),..."""
#     cursor = connection.cursor()
#     try:
#         extras.execute_values(cursor, query, tuples)

#         connection.commit()
#         print("the dataframe is inserted")
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)

# finally:

#     connection.close()
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


if __name__ == "__main__":
    main()
