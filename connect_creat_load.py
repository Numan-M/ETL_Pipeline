import pandas as pd
import psycopg2
import psycopg2.extras as extras
def main():
    connection = {"database":"test", "user" : "postgres", "password" :"pass", "host" : "localhost", "port":"5432"}
    """
    This function is the main where i execute all my sub functions which are seen below 
    """
    connecting_to_db = connect_to_database(connection)

    query =  ('''
        CREATE TABLE IF NOT EXISTS orders(
        order_id SERIAL PRIMARY KEY not null,
        timestamp date not null,
        store_name text not null,
        total_price float not null,
        payment_method text not null
        
        );
        ''')
    query_2 = '''
        CREATE TABLE IF NOT EXISTS products_table(
        product_id SERIAL primary key not null,
        products text not null,
        total_price float not null
          );
        '''
    create_table(connecting_to_db, query)
    create_table(connecting_to_db, query_2)
    drop_colums_1 = ["customer_name","card_number","products"]
    drop_colums_2 = ["store_name", "payment_method","card_number","customer_name", "timestamp"]

    df = pd.read_csv('mockFileWithHeaders.csv')
    df2 = pd.read_csv('mockFileWithHeaders.csv')
    df.drop(columns=drop_colums_1, axis= 1, inplace=True)
    df2.drop(columns= drop_colums_2, axis= 1, inplace= True)
    df['timestamp'] = pd.to_datetime(df["timestamp"])
    

    insert_values_in_table(connecting_to_db, df, 'orders')
    insert_values_in_table(connecting_to_db,df2,'products_table')


def connect_to_database(connection):
    """
    function to connect the the database, has 1 arguemnt connection which is a dict for the
    parameters to connect to the database.
    If successful it will return conn
    """
    try:
        print("connecting")
        conn = psycopg2.connect(**connection) 
        print("done")
    except:
        print("not done")
    return conn


def create_table(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)



def insert_values_in_table(conn, df, table_used):
    """
    function below takes three arguemnts , connnection, dataframe and table name inside connection.
    tuples--> creating a list of tuples from the dataframe. Has to be pass in values inside database
    cols --> columns are comma-separated
    query --> inserting
    """

    tuples = [tuple(x) for x in df.to_numpy()]


    columns = ','.join(list(df.columns))
    
    query = "INSERT INTO %s(%s) VALUES %%s" % (table_used, columns)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        print("the dataframe is inserted")

connect_to_database.commit()
connect_to_database.close()

if __name__ == "__main__":
    """
    this runs the main function 
    """
    main() 