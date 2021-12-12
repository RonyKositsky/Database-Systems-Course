import csv
from modifiedconnector import ModifiedConnector

connector = ModifiedConnector()


def create_db_tables():
    """
        Creating the tables in our database.
        First, it will delete any possible existing table, then it will create them.
    """
    # remove all existing tables
    table_names = ["movies", "movies_votes", "actors", "ratings"]
    for name in table_names:
        cmd = f'''DROP TABLE {name};'''
        try:
            connector.execute(cmd)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    # create all tables
    cmd = '''CREATE TABLE movies (movie_id INT, title VARCHAR(20), year INT, genre VARCHAR(15), duration INT, 
            country VARCHAR(15), language VARCHAR(15), production VARCHAR(30), 
            actors VARCHAR(50), description VARCHAR(100)); '''
    try:
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    cmd = '''CREATE TABLE movies_votes (movie_id INT, avg_vote FLOAT, votes INT, budget INT ); '''
    try:
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    # cursor = cnx.cursor()
    cmd = '''CREATE TABLE actors (actor_id INT, name VARCHAR(20), birth_name VARCHAR(20), height INT, 
            bio VARCHAR(100), birth_date DATE, death_date DATE, divorces INT, children INT); '''
    try:
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

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
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def insert_data():
    in_f = open("movies.csv", encoding="cp437", mode="r")
    csv_reader = csv.reader(in_f, delimiter=',')
    cmd = ("INSERT INTO movies (movie_id, title, year, genre, duration, country, language, production, actors, "
           "description) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)")
    for row in csv_reader:
        try:
            movie_id = int(row[0][2:])
            title = row[1]
            year = int(row[2])
            genre = row[3].split(',')[0]
            duration = int(row[4])
            country = row[5]
            language = row[6]
            production = row[7]
            actors = row[8]
            description = row[9]
            data = (movie_id, title, year, genre, duration, country, language, production, actors, description)
            connector.inset_data(cmd, data)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def modifications():
    cmd = '''ALTER TABLE movies MODIFY actors VARCHAR(1000);'''
    connector.execute(cmd)


if __name__ == "__main__":
    # create_db_tables()
    insert_data()
    # modifications()
    #connector.close()
