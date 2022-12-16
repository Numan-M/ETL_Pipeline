import psycopg2
from connect_to_db import connecting_to_db




def create_table(conn,query):
    cursor = conn.cursor()
    
    cursor.execute(query)


script_store ='''CREATE TABLE IF NOT EXISTS stores(
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(100) UNIQUE NOT NULL
 
    );'''

script_payment = '''CREATE TABLE IF NOT EXISTS payment_methods(
    payment_id SERIAL PRIMARY KEY,
    payment_method VARCHAR(100)  UNIQUE NOT NULL
    );'''
    

query_transaction = '''CREATE TABLE IF NOT EXISTS transactions (
    transaction_id SERIAL PRIMARY KEY,
    store_id INT,
    timestamp TIMESTAMP NOT NULL,
    total_price MONEY  NOT NULL,
    FOREIGN KEY(store_id)
    REFERENCES stores(store_id)

    
    );'''

query_products = '''CREATE TABLE IF NOT EXISTS products(
    product_id SERIAL NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    store_name VARCHAR(100) NOT NULL,
    size VARCHAR(100) NOT NULL,
    product_type VARCHAR (100) NOT NULL,
    price MONEY NOT NULL
    );'''

alter_t='''UPDATE transactions
SET store_id = stores.store_id
FROM stores
WHERE transactions.store_id is null;'''

alter_t1='''UPDATE transactions
SET payment_id = payment_methods.payment_id
FROM payment_methods
WHERE transactions.payment_id is null;'''



#select * from stores join transactions on transactions.store_id = stores.store_id

# query_orders = '''CREATE TABLE IF NOT EXIST orders (
#     "order_id" SERIAL NOT NULL,
#     "transaction_id" INT NOT NULL,
#     "product_id" INT NOT NULL,
#     CONSTRAINT pk_Orders
#      PRIMARY KEY order_id
     
# );'''

# query_stores = '''CREATE TABLE "Stores" (
#     "store_id" SERIAL   NOT NULL,
#     "store_name" VARCHAR(100)   NOT NULL,
#     CONSTRAINT pk_Stores
#     PRIMARY KEY store_id
    
# );'''

# query_products = '''CREATE TABLE Products (
#     "product_id" SERIAL NOT NULL,
#     "product_size_and_type" VARCHAR(200) NOT NULL,
#     "flavour_id" INT NULL,
#     "price" MONEY NOT NULL,
#     CONSTRAINT pk_Products
#     PRIMARY KEY  product_id

    
# );'''

# query_flavours = '''CREATE TABLE Flavours (
#     "flavour_id" SERIAL   NOT NULL,
#     "flavour" VARCHAR(50)   NOT NULL,
#     CONSTRAINT "pk_Flavours" PRIMARY KEY (
#         "flavour_id"
#      ),
#     CONSTRAINT "uc_Flavours_flavour" UNIQUE (
#         "flavour"
#     )
# );'''

# query_payment_methods = '''CREATE TABLE Payment_methods (
#     "payment_method_id" SERIAL   NOT NULL,
#     "payment_method" VARCHAR(20)   NOT NULL,
#     CONSTRAINT "pk_Payment_method" PRIMARY KEY (
#         "payment_method_id"
#      ),
#     CONSTRAINT "uc_Payment_method_payment_methods" UNIQUE (
#         "payment_method"
#     )
# );'''

 
# '''ALTER TABLE Transactions ADD CONSTRAINT fk_Transactions_store_id FOREIGN KEY(store_id)
# REFERENCES Stores (store_id);'''

# ALTER TABLE "Transactions" ADD CONSTRAINT "fk_Transactions_payment_method_id" FOREIGN KEY("payment_method_id")
# REFERENCES "Payment_method" ("payment_method_id");

# ALTER TABLE "Orders" ADD CONSTRAINT "fk_Orders_transaction_id" FOREIGN KEY("transaction_id")
# REFERENCES "Transactions" ("transaction_id");

# ALTER TABLE "Orders" ADD CONSTRAINT "fk_Orders_product_id" FOREIGN KEY("product_id")
# REFERENCES "Products" ("product_id");

# ALTER TABLE "Products" ADD CONSTRAINT "fk_Products_flavour_id" FOREIGN KEY("flavour_id")
# REFERENCES "Flavours" ("flavour_id");

