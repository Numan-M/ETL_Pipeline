import psycopg2


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


connection = {
    "database": "test",
    "user": "postgres",
    "password": "pass",
    "host": "localhost",
    "port": "5432",
}
connecting_to_db = connect_to_database(connection)
