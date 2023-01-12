import psycopg2

from src.connecting import connecting_to_db


# def create_table(conn,query):
#     cursor = conn.cursor()

#     cursor.execute(query)


query_store = """CREATE TABLE IF NOT EXISTS stores(
    store_id SERIAL PRIMARY KEY NOT NULL,
    store_name VARCHAR(100) UNIQUE NOT NULL
    );"""

query_payment = """CREATE TABLE IF NOT EXISTS payment_methods(
    payment_method_id SERIAL PRIMARY KEY NOT NULL,
    payment_method VARCHAR(100)  UNIQUE NOT NULL
    );"""


query_products = """CREATE TABLE IF NOT EXISTS products(
    product_id SERIAL PRIMARY KEY NOT NULL,
    product_name VARCHAR(200) UNIQUE NOT NULL,
    price MONEY NOT NULL
    );"""

query_customer_basket = """CREATE TABLE IF NOT EXISTS customer_basket(
    customer_basket_id SERIAL PRIMARY KEY not NULL,
    store_id INT not null,
    payment_method_id INT not null,
    timestamp timestamp not null,
    total_price MONEY not null
    );"""

query_sales = """CREATE TABLE IF NOT EXISTS sales(
    sales_id serial primary key not null,
    customer_basket_id INT not null,
    product_id INT not null
    );"""


alter_customer_basket_storeid = """ALTER TABLE customer_basket
                        ADD CONSTRAINT fk_customer_basket_store_id FOREIGN KEY(store_id) REFERENCES stores(store_id);"""

alter_customer_basket_paymentid = """ALTER TABLE customer_basket
                        ADD CONSTRAINT fk_customer_basket_payment_method_id FOREIGN KEY(payment_method_id) REFERENCES payment_methods(payment_method_id);"""

alter_sales_customer_basket_id = """ALTER TABLE sales
                        ADD CONSTRAINT fk_sales_customer_basket_id FOREIGN KEY(customer_basket_id) REFERENCES customer_basket(customer_basket_id);"""
alter_sales_product_id = """ALTER TABLE sales
                        ADD CONSTRAINT fk_sales_product_id FOREIGN KEY(product_id) REFERENCES products(product_id);"""
