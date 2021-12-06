import mysql.connector

cnx = mysql.connector.connect(
    host='127.0.0.1',
    port=3305,
    user='DbMysql03',
    password='DbMysql03',
    database='DbMysql03',
    )

mycursor = cnx.cursor()
mycursor.execute("CREATE TABLE test (name VARCHAR(255), address VARCHAR(255))")

cnx.close()
