import redshift_connector

from src.connecting import connection


create_transactions_table = """CREATE TABLE IF NOT EXISTS transactions2 (
    transaction_id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    store_id INT,
    payment_method_id INT,
    total_price NUMERIC(5,2),
    CONSTRAINT fk_stores
        FOREIGN KEY(store_id)
            REFERENCES stores(store_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT fk_payment_methods
        FOREIGN KEY(payment_method_id)
            REFERENCES payment_methods(payment_method_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    );
    """

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

query_customer_basket = """CREATE TABLE IF NOT EXISTS customer_basket(
    customer_basket_id INT IDENTITY(1,1) PRIMARY KEY not NULL,
    store_id INT not null,
    payment_method_id INT not null,
    timestamp timestamp not null,
    total_price NUMERIC(5,2) not null,
    CONSTRAINT fk_stores
        FOREIGN KEY(store_id)
            REFERENCES stores(store_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    CONSTRAINT fk_payment_methods
        FOREIGN KEY(payment_method_id)
            REFERENCES payment_methods(payment_method_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    );"""

query_sales = """CREATE TABLE IF NOT EXISTS sales(
    sales_id INT IDENTITY(1,1) primary key not null,
    customer_basket_id INT not null,
    product_id INT not null,
    CONSTRAINT fk_sales_customer_basket_id
        FOREIGN KEY(customer_basket_id)
            REFERENCES customer_basket(customer_basket_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    CONSTRAINT fk_sales_product_id
        FOREIGN KEY(product_id)
            REFERENCES products(product_id)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    );"""

query_create_log_files = """CREATE TABLE IF NOT EXISTS file_log (
    file_name VARCHAR(100) UNIQUE NOT NULL PRIMARY KEY, time_logged TIMESTAMP);"""
