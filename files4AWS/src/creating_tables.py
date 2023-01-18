import redshift_connector

from src.connecting import connection


query_store = """CREATE TABLE IF NOT EXISTS stores(
    store_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    store_name VARCHAR(100) UNIQUE NOT NULL
    );"""

query_payment = """CREATE TABLE IF NOT EXISTS payment_methods(
    payment_method_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    payment_method VARCHAR(100)  UNIQUE NOT NULL
    );"""


query_products = """CREATE TABLE IF NOT EXISTS products(
    product_id INT IDENTITY(1,1) PRIMARY KEY NOT NULL,
    product_name VARCHAR(200) UNIQUE NOT NULL,
    price NUMERIC(5,2) NOT NULL
    );"""

query_transactions = """CREATE TABLE IF NOT EXISTS transactions(
    transaction_id INT IDENTITY(1,1) PRIMARY KEY not NULL,
    store_id INT not null,
    payment_method_id INT not null,
    timestamp timestamp not null,
    total_price NUMERIC(5,2) not null,
    CONSTRAINT fk_stores
        FOREIGN KEY(store_id)
            REFERENCES stores(store_id)
    CONSTRAINT fk_payment_methods
        FOREIGN KEY(payment_method_id)
            REFERENCES payment_methods(payment_method_id)
    );"""

query_sales = """CREATE TABLE IF NOT EXISTS sales(
    sales_id INT IDENTITY(1,1) primary key not null,
    transaction_id INT not null,
    product_id INT not null,
    CONSTRAINT fk_sales_transaction_id
        FOREIGN KEY(transaction_id)
            REFERENCES transactions(transaction_id)
    CONSTRAINT fk_sales_product_id
        FOREIGN KEY(product_id)
            REFERENCES products(product_id)
    );"""

query_create_log_files = """CREATE TABLE IF NOT EXISTS file_log (
    file_name VARCHAR(100) UNIQUE NOT NULL PRIMARY KEY, time_logged TIMESTAMP);"""
