import mysql.connector
import csv

cnx = mysql.connector.connect(
    host='127.0.0.1',
    port=3305,
    user='DbMysql03',
    password='DbMysql03',
    database='DbMysql03',
    )

def createDBTables():
    ### remove all exicting tables ###
    cursor = cnx.cursor()
    cmd = '''DROP TABLE movies; '''
    try:
        cursor.execute(cmd)
        cnx.commit()
    except:
        print('Error during removing movies')
        cnx.rollback()

    #cursor = cnx.cursor()
    cmd = '''DROP TABLE movies_votes; '''
    try:
        cursor.execute(cmd)
        cnx.commit()
    except:
        print('Error during removing movies_votes')
        cnx.rollback()

    #cursor = cnx.cursor()
    cmd = '''DROP TABLE actors; '''
    try:
        cursor.execute(cmd)
        cnx.commit()
    except:
        print('Error during removing actors')
        cnx.rollback()

    #cursor = cnx.cursor()
    cmd = '''DROP TABLE ratings; '''
    try:
        cursor.execute(cmd)
        cnx.commit()
    except:
        print('Error during removing ratings')
        cnx.rollback()

    ### create all tables ###
    #cursor = cnx.cursor()
    cmd = '''CREATE TABLE movies (movie_id INT, title VARCHAR(20), year INT, genre VARCHAR(15), duration INT, 
            country VARCHAR(15), language VARCHAR(15), production VARCHAR(30), 
            actors VARCHAR(50), description VARCHAR(100)); '''
    try:
        cursor.execute(cmd)
        cnx.commit()
    except:
        print('Error during creating movies')
        cnx.rollback()

    #cursor = cnx.cursor()
    cmd = '''CREATE TABLE movies_votes (movie_id INT, avg_vote FLOAT, votes INT, budget INT ); '''
    try:
        cursor.execute(cmd)
        cnx.commit()
    except:
        print('Error during creating movies_votes')
        cnx.rollback()

    #cursor = cnx.cursor()
    cmd = '''CREATE TABLE actors (actor_id INT, name VARCHAR(20), birth_name VARCHAR(20), height INT, 
            bio VARCHAR(100), birth_date DATE, death_date DATE, divorces INT, children INT); '''
    try:
        cursor.execute(cmd)
        cnx.commit()
    except:
        print('Error during creating actors')
        cnx.rollback()

    #cursor = cnx.cursor()
    cmd = '''CREATE TABLE ratings (movie_id INT, weighted_average_vote FLOAT, mean_vote FLOAT, median_vote INT,
            allgenders_0age_avg_vote FLOAT, allgenders_0age_votes INT, allgenders_18age_avg_vote FLOAT,
            allgenders_18age_votes INT, allgenders_30age_avg_vote FLOAT, allgenders_30age_votes INT,
            allgenders_45age_avg_vote FLOAT, allgenders_45age_votes INT, males_allages_avg_vote FLOAT,
            males_allages_votes INT, males_0age_avg_vote FLOAT, males_0age_votes INT, males_18age_avg_vote FLOAT,
            males_18age_votes INT, males_30age_avg_vote FLOAT, males_30age_votes INT, males_45age_avg_vote FLOAT,
            males_45age_votes INT, females_allages_avg_vote FLOAT, females_allages_votes INT,
            females_0age_avg_vote FLOAT, females_0age_votes INT, females_18age_avg_vote FLOAT,
            females_18age_votes INT, females_30age_avg_vote FLOAT, females_30age_votes INT, 
            females_45age_avg_vote FLOAT, females_45age_votes INT, top1000_voters_rating FLOAT, top1000_voters_votes INT,
            us_voters_rating FLOAT); '''
    try:
        cursor.execute(cmd)
        cnx.commit()
    except:
        print('Error during creating ratings')
        cnx.rollback()


def insert_data():
    # movies file
    line_cnt = 0
    in_f = open("IMDB movies.csv", encoding="cp437", mode="r")
    csv_reader = csv.reader(in_f, delimiter=',')
    cmd = ("INSERT INTO movies (movie_id, title, year, genre, duration, country, language, production, actors ) "
           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    cursor = cnx.cursor()
    for row in csv_reader:
        if line_cnt == 1000:
            break
        else:
            try:
                movie_id = row[0]
                title = row[1]
                year = row[3]
                genre = row[5].split(',')[0]
                duration = row[6]
                country = row[7]
                language = row[8]
                production = row[11]
                actors = row[12]
                cursor.execute(cmd, (movie_id, title, year, genre, duration, country, language, production, actors))
                cnx.commit()
                line_cnt += 1
            except:
                print("problematic line")
                print(row)
                cnx.rollback()
    cnx.close()



#createDBTables()

'''cursor = cnx.cursor()
cmd = ("INSERT INTO movies (movie_id, title, year, genre, duration, country, language, production, actors ) "
       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
try:
    cursor.execute(cmd, ("1", "2", "3", "4", "5", "6", "7", "8", "9"))
    cnx.commit()
except:
    print("Error")
    cnx.rollback()

cnx.close()'''
insert_data()


