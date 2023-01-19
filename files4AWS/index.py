from copy import deepcopy

import pandas as pd
import redshift_connector

from src.connecting import connecting_to_db
from src.creating_tables import query_payment
from src.creating_tables import query_products
from src.creating_tables import query_sales
from src.creating_tables import query_store
from src.creating_tables import query_transactions
from src.g5_lambda_1 import bucket_name
from src.g5_lambda_1 import get_values_already_present
from src.g5_lambda_1 import process_file_for_loading

# from src.g5_lambda_1 import collect_names_of_files_in_bucket
# from src.g5_lambda_1 import process_list_of_files
# from src.g5_lambda_1 import get_present_pay_methods


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
        print("MELON2")
        cursor = connecting_to_db.cursor()

        run_query(cursor, query_store)
        run_query(cursor, query_payment)
        run_query(cursor, query_products)
        run_query(cursor, query_transactions)
        run_query(cursor, query_sales)

        products = {
            "Latte": [2.15, 2.45],
            "Flavoured latte - Vanilla": [2.55, 2.85],
            "Flavoured latte - Caramel": [2.55, 2.85],
            "Flavoured latte - Hazelnut": [2.55, 2.85],
            "Flavoured latte - Gingerbread": [2.55, 2.85],
            "Cappuccino": [2.15, 2.45],
            "Americano": [1.95, 2.25],
            "Flat white": [2.15, 2.45],
            "Cortado": [2.05, 2.35],
            "Mocha": [2.30, 2.70],
            "Espresso": [1.50, 1.80],
            "Filter coffee": [1.50, 1.80],
            "Chai latte": [2.30, 2.60],
            "Hot chocolate": [2.20, 2.90],
            "Flavoured hot chocolate - Caramel": [2.60, 2.90],
            "Flavoured hot chocolate - Hazelnut": [2.60, 2.90],
            "Flavoured hot chocolate - Vanilla": [2.60, 2.90],
            "Luxury hot chocolate": [2.40, 2.70],
            "Red Label tea": [1.20, 1.80],
            "Speciality Tea - Earl Grey": [1.30, 1.60],
            "Speciality Tea - Green": [1.30, 1.60],
            "Speciality Tea - Camomile": [1.30, 1.60],
            "Speciality Tea - Peppermint": [1.30, 1.60],
            "Speciality Tea - Fruit": [1.30, 1.60],
            "Speciality Tea - Darjeeling": [1.30, 1.60],
            "Speciality Tea - English breakfast": [1.30, 1.60],
            "Iced latte": [2.35, 2.85],
            "Flavoured iced latte - Vanilla": [2.75, 3.25],
            "Flavoured iced latte - Caramel": [2.75, 3.25],
            "Flavoured iced latte - Hazelnut": [2.75, 3.25],
            "Iced americano": [2.15, 2.50],
            "Frappes - Chocolate Cookie": [2.75, 3.25],
            "Frappes - Strawberries & Cream": [2.75, 3.25],
            "Frappes - Coffee": [2.75, 3.25],
            "Smoothies - Carrot Kick": [2.00, 2.50],
            "Smoothies - Berry Beautiful": [2.00, 2.50],
            "Smoothies - Glowing Greens": [2.00, 2.50],
            "Hot Chocolate": [1.40, 1.70],
            "Glass of milk": [0.70, 1.10],
        }

        all_product_dict = {}
        for key, value in products.items():
            all_product_dict[f"Regular {key}"] = value[0]
            all_product_dict[f"Large {key}"] = value[1]

        products_df = pd.DataFrame(
            all_product_dict.items(), columns=["product_name", "price"]
        )

        dict_of_tables = process_file_for_loading(object_key, cursor)
        # list of tables is a dictionary
        # {"customer_basket_table" : customer_basket_table, "payment_methods_table" : payment_methods_table,
        # "products_table" : products_table, "sales_table" : sales_table, "store_name_table" : store_name_table}

        present_stores = get_values_already_present(cursor, "stores", "store_name")
        frame_to_insert_new_stores = delete_duplicates_if_already_in_db(
            dict_of_tables["store_name_table"], present_stores
        )
        if not frame_to_insert_new_stores.empty:
            insert_values_in_table(cursor, frame_to_insert_new_stores, "stores")
            print("Inserted into stores")

        present_pay_methods = get_values_already_present(
            cursor, "payment_methods", "payment_method"
        )
        frame_to_insert_new_payment_methods = delete_duplicates_if_already_in_db(
            dict_of_tables["payment_methods_table"], present_pay_methods
        )
        if not frame_to_insert_new_payment_methods.empty:
            insert_values_in_table(
                cursor, frame_to_insert_new_payment_methods, "payment_methods"
            )
            print("Inserted into payment methods")

        present_products = get_values_already_present(
            cursor, "products", "product_name"
        )

        if get_values_already_present(cursor, "products", "product_name") == []:
            insert_values_in_table(cursor, products_df, "products")
            print("Inserted into products")

        cursor.execute(f"SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        count_variable = int(count)
        dict_of_tables["sales_table"]["transaction_id"] += count_variable
        # print("Trying to inserted into transactions")
        # load_transactions_to_db(cursor, dict_of_tables["transactions_table"])
        column_as_list = dict_of_tables["sales_table"].product_name.values.tolist()
        column_as_list_of_unique_values = set(column_as_list)
        for value in column_as_list_of_unique_values:
            cursor.execute(
                f"""SELECT product_id FROM products WHERE products.product_name = '{value}'"""
            )
            product_id = cursor.fetchone()[0]
            dict_of_tables["sales_table"]["product_name"] = dict_of_tables[
                "sales_table"
            ]["product_name"].replace([value], product_id)
        dict_of_tables["sales_table"] = dict_of_tables["sales_table"].rename(
            columns={"product_name": "product_id"}
        )

        insert_values_in_table(cursor, dict_of_tables["sales_table"], "sales")

        # print("Inserted into transactions")

        # load_sales_to_db(cursor, dict_of_tables["sales_table"])

        # customer_basket_new_frame = dict_of_tables["customer_basket_table"]
        # customer_basket_new_frame.replace(store_id, store_id+1)
        # customer_basket_new_frame.at[0,'NAME']='Safa'
        # insert_values_in_table(cursor, dict_of_tables["transactions_table"], "transactions")
        # print(dict_of_tables["sales_table"])

        print("Trying to inserted into sales")
        # insert_values_in_table(cursor, dict_of_tables["sales_table"], "sales")
        print("Inserted into sales")

        # THIS IS TO replace store names with store ids in transactions table
        column_as_list = dict_of_tables["transactions_table"].store_name.values.tolist()
        column_as_list_of_unique_values = set(column_as_list)
        for value in column_as_list_of_unique_values:
            cursor.execute(
                f"""SELECT store_id FROM stores WHERE stores.store_name = '{value}'"""
            )
            store_id = cursor.fetchone()[0]
            dict_of_tables["transactions_table"]["store_name"] = dict_of_tables[
                "transactions_table"
            ]["store_name"].replace([value], store_id)
        dict_of_tables["transactions_table"] = dict_of_tables[
            "transactions_table"
        ].rename(columns={"store_name": "store_id"})

        # THIS IS TO replace payment method  with payment ids in transactions table
        column_as_list = dict_of_tables[
            "transactions_table"
        ].payment_method.values.tolist()
        column_as_list_of_unique_values = set(column_as_list)
        for value in column_as_list_of_unique_values:
            cursor.execute(
                f"""SELECT payment_method_id FROM payment_methods WHERE payment_methods.payment_method = '{value}'"""
            )
            payment_method_id = cursor.fetchone()[0]
            dict_of_tables["transactions_table"]["payment_method"] = dict_of_tables[
                "transactions_table"
            ]["payment_method"].replace([value], payment_method_id)
        dict_of_tables["transactions_table"] = dict_of_tables[
            "transactions_table"
        ].rename(columns={"payment_method": "payment_method_id"})

        insert_values_in_table(
            cursor, dict_of_tables["transactions_table"], "transactions"
        )

        print(dict_of_tables["sales_table"])

        cursor.close()
        connecting_to_db.close()

    def load_transactions_to_db(cursor, df):
        # create connection

        tuples = [
            tuple(row) for row in df.to_numpy()
        ]  # create a tuple for each row of data, ready to insert

        try:
            for row in tuples:
                print("155")
                cursor.execute(
                    f"""SELECT store_id FROM stores WHERE stores.store_name = '{row[1]}'"""
                )
                print("157")
                store_id = cursor.fetchone()[0]
                print(f"store id is {store_id}")
                print("159")
                cursor.execute(
                    f"""SELECT payment_method_id FROM payment_methods WHERE payment_methods.payment_method = '{row[3]}'"""
                )
                print("161")
                payment_method_id = cursor.fetchone()[0]
                print(f"payment method id is {payment_method_id}")
                print("163")
                cursor.execute(
                    f"""INSERT INTO transactions(timestamp, store_id, total_price, payment_method_id) VALUES ('{row[0]}',
                {store_id}, {row[2]}, {payment_method_id});"""
                )
                print("168")
            connecting_to_db.commit()
        except (Exception, redshift_connector.DatabaseError) as error:
            print("Error: %s" % error)
            return
        print("The data has been inserted")

    def load_sales_to_db(cursor, df):
        # create connection

        tuples = [
            tuple(row) for row in df.to_numpy()
        ]  # create a tuple for each row of data, ready to insert
        print("162 has run")
        try:
            print("165 has run")
            for row in tuples:
                print("167 has run")

                cursor.execute(
                    f"""SELECT transaction_id FROM transactions WHERE transactions.transaction_id = '{row[0]}'"""
                )
                transaction_id = cursor.fetchone()[0]
                cursor.execute(
                    f"""SELECT product_id FROM products WHERE products.product_name = '{row[1]}'"""
                )
                product_id = cursor.fetchone()[0]
                cursor.execute(
                    f"""INSERT INTO sales(transaction_id,product_id) VALUES ({transaction_id}, {product_id});"""
                )
                print("174 has run")
            connecting_to_db.commit()
            print("176 has run")
        except (Exception, redshift_connector.DatabaseError) as error:
            print("Error: %s" % error)
            return
        print("The data has been inserted")

    def delete_duplicates_if_already_in_db(frame_to_insert, list_of_present_values):
        # clean_frame_to_insert = deepcopy(frame_to_insert)
        for value in list_of_present_values:
            if value in frame_to_insert.values:
                frame_to_insert = frame_to_insert[frame_to_insert.values != value]
                # clean_frame_to_insert = clean_frame_to_insert[clean_frame_to_insert.values != value ]
        return frame_to_insert

    def run_query(cursor, query):
        try:
            cursor.execute(query)
            connecting_to_db.commit()
            print("Successfully run query")
        except (Exception, redshift_connector.DatabaseError) as error:
            print(f"Following query failed {query}")
            print(error)

    def insert_values_in_table(cursor, df, table_name):
        """Loading data into DB"""
        columns = ",".join(list(df.columns))
        asterisk = ", ".join(["%s"] * len(df.columns))

        query = f"INSERT INTO public.{table_name} ({columns}) VALUES ({asterisk});"
        """--> INSERT INTO payment_methods(payment_method) VALUES ('CARD'),..."""

        cursor = connecting_to_db.cursor()

        try:
            for index, row in df.iterrows():
                values = tuple(row)
                cursor.execute(query, values)
            connecting_to_db.commit()
            print(f"A DATAFRAME HAS BEEN INSERTED")
            return

        except (Exception, redshift_connector.DatabaseError) as error:
            print("THERE WAS AN ERROR INSERTING")
            print(f"QUERY TO INSERT IS {query}")
            print(error)

    main()
    # if __name__ == "__main__":
    #    main()
