import psycopg2

conn = psycopg2.connect(database="group", user = "postgres", password = "pass", host = "localhost", port = "5432")

if conn:
    print("done")




