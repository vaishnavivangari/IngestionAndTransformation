import mysql.connector
from configFileReader import user,password,host,dbName,sqlQuery

# Prepare MySql connection.
connection = mysql.connector.connect(
    user=user,
    password=password,
    host=host
)
# create mycursor object to make the connection for executing SQL queries
mycursor = connection.cursor()

with open(sqlQuery, 'r') as sql_file:
    result_iterator = mycursor.execute(sql_file.read(), multi=True)
    for res in result_iterator:
        # Will print out a short representation of the query
        print("Running query: ", res)
        print(f"Affected {res.rowcount} rows")

    connection.commit()