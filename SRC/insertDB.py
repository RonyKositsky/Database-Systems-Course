import csv
from os import close
import MySQLdb

def createDBTables():
    db = MySQLdb.connect("", "","","")
    cursor = db.cursor()
    cmd = '''CREATE TABLE movies (movie_id int, title vchar(20), year int, genre vchar(15), duration int, country vchar(15), language vchar(15), production vchar(30), 
            actors vchar(50), description vchar(100), avg_vote int, votes int, budget int
            PRIMARY KEY (movie_id)); '''
    try: 
        cursor.execute(cmd)
        db.commit()
    except:
        db.rollback()
    db.close()
