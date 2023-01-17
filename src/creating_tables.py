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
    payment_method_id INT PRIMARY KEY,
    payment_method VARCHAR(100)  UNIQUE NOT NULL
    );"""


query_products = """CREATE TABLE IF NOT EXISTS products(
    product_id SERIAL PRIMARY KEY NOT NULL,
    product_name VARCHAR(200) UNIQUE NOT NULL,
    price MONEY NOT NULL
    );"""

query_customer_basket = """CREATE TABLE IF NOT EXISTS customer_basket(
    customer_basket_id SERIAL PRIMARY KEY not NULL,
    store_name VARCHAR(30) not null,
    payment_method VARCHAR(50) not null,
    timestamp timestamp not null,
    total_price MONEY not null
    );"""

query_sales = """CREATE TABLE IF NOT EXISTS sales(
    sales_id serial primary key not null,
    customer_basket_id INT not null,
    product_name VARCHAR(200) not null
    );"""
