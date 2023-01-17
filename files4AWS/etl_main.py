import pandas as pd
import redshift_connector

from src.connecting import connect_to_database
from src.creating_tables import query_create_log_files
from src.creating_tables import query_customer_basket
from src.creating_tables import query_payment
from src.creating_tables import query_products
from src.creating_tables import query_sales
from src.creating_tables import query_store
from src.g5_lambda_1 import bucket_name
from src.g5_lambda_1 import collect_names_of_files_in_bucket
from src.g5_lambda_1 import process_list_of_files

# from src.g5_lambda_1 import customer_basket_table
# from src.g5_lambda_1 import payment_methods_table
# from src.g5_lambda_1 import products_table
# from src.g5_lambda_1 import sales_tablblee
# from src.g5_lambda_1 import store_name_ta


connection_details = {
    "database": "group5_cafe",
    "user": "group5",
    "password": "Redshift-delon8-group5-76sdhghs",
    "host": "redshiftcluster-bie5pcqdgojl.cje2eu9tzolt.eu-west-1.redshift.amazonaws.com",
    "port": 5439,
}


def handler(event, context):

    object_key = event["Records"][0]["s3"]["object"]["key"]

    def main():

        connection = connect_to_database(connection_details)

        run_query(connection, query_create_log_files)
        run_query(connection, query_store)
        run_query(connection, query_payment)
        run_query(connection, query_products)
        run_query(connection, query_customer_basket)
        run_query(connection, query_sales)

        # run_query(connection, alter_customer_basket_storeid)
        # run_query(connection, alter_customer_basket_paymentid)

        # run_query(connection, alter_sales_customer_basket_id)
        # run_query(connection, alter_sales_product_id)

        list_of_file_names = collect_names_of_files_in_bucket(bucket_name)
        dict_of_tables = process_list_of_files(list_of_file_names)
        print(dict_of_tables)
        # list of tables is a dictionary
        # {"customer_basket_table" : customer_basket_table, "payment_methods_table" : payment_methods_table,
        # "products_table" : products_table, "sales_table" : sales_table, "store_name_table" : store_name_table}

        insert_values_in_table(connection, dict_of_tables["store_name_table"], "stores")
        insert_values_in_table(
            connection, dict_of_tables["payment_methods_table"], "payment_methods"
        )
        print("NEXT TRIES TO INSERT PRODUCTS TABLE")
        insert_values_in_table(connection, dict_of_tables["products_table"], "products")
        print("NEXT TRIES TO INSERT CUSTOMER BASKET TABLE")
        insert_values_in_table(
            connection, dict_of_tables["customer_basket_table"], "customer_basket"
        )
        insert_values_in_table(connection, dict_of_tables["sales_table"], "sales")

        connection.close()

    def run_query(connection, query):
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            print(f"Successfully run query: {query} ")
            connection.commit()

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
            print(f"THE DATAFRAME {df} IS INSERTED")

        except (Exception, redshift_connector.DatabaseError) as error:
            print("THERE WAS AN ERROR INSERTING")
            print(f"QUERY TO INSERT IS {query}")
            print(error)

        # finally:
        #     connection.close()

    main()
    # if __name__ == "__main__":
    #    main()
