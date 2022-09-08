import psycopg2


conn = psycopg2.connect(host="localhost",
    database="smartbus",
    user="smartbus",
    password="smartbus")

cur = conn.cursor()

print('PostgreSQL database version:')
cur.execute('SELECT version()')

# display the PostgreSQL database server version
db_version = cur.fetchone()
print(db_version)

# close the communication with the PostgreSQL
cur.close()