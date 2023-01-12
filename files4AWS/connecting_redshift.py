import redshift_connector


def connect_to_database(connection):
    """
    function to connect the the database, has 1 arguemnt connection which is a dict for the
    parameters to connect to the database.
    If successful it will return conn
    """
    try:
        print("connecting")
        conn = redshift_connector.connect(**connection)
        print("Connection done")
    except Exception as e:
        print(e)
        print("not done")
    return conn


connection = {
    "database": "group5_cafe",
    "user": "group5",
    "password": "Redshift-delon8-group5-76sdhghs",
    "host": "redshiftcluster-bie5pcqdgojl.cje2eu9tzolt.eu-west-1.redshift.amazonaws.com",
    "port": 5439,
}

connecting_to_db = connect_to_database(connection)
