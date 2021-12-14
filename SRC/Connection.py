import csv
from ModifiedConnector import ModifiedConnector
from datetime import datetime

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

    cmd = '''CREATE TABLE movies (movie_id INT, title VARCHAR(1000), year INT, genre VARCHAR(1000), duration INT, 
            country VARCHAR(1000), language VARCHAR(1000), production VARCHAR(30), 
            actors VARCHAR(1000), description VARCHAR(1000)); '''
    try:
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    cmd = '''CREATE TABLE movies_votes (movie_id INT, avg_vote FLOAT, votes INT, budget INT ); '''
    try:
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    cmd = '''CREATE TABLE actors (actor_id INT, name VARCHAR(100), birth_name VARCHAR(100), height INT, 
            bio VARCHAR(16383), birth_date DATE, death_date DATE, divorces INT, children INT); '''
    try:
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    cmd = '''CREATE TABLE ratings (movie_id INT, weighted_average_vote FLOAT, mean_vote FLOAT, median_vote INT, 
    allgenders_0age_avg_vote FLOAT, allgenders_0age_votes INT, allgenders_18age_avg_vote FLOAT, 
    allgenders_18age_votes INT, allgenders_30age_avg_vote FLOAT, allgenders_30age_votes INT, 
    allgenders_45age_avg_vote FLOAT, allgenders_45age_votes INT, males_allages_avg_vote FLOAT, males_allages_votes 
    INT, males_0age_avg_vote FLOAT, males_0age_votes INT, males_18age_avg_vote FLOAT, males_18age_votes INT, 
    males_30age_avg_vote FLOAT, males_30age_votes INT, males_45age_avg_vote FLOAT, males_45age_votes INT, 
    females_allages_avg_vote FLOAT, females_allages_votes INT, females_0age_avg_vote FLOAT, females_0age_votes INT, 
    females_18age_avg_vote FLOAT, females_18age_votes INT, females_30age_avg_vote FLOAT, females_30age_votes INT, 
    females_45age_avg_vote FLOAT, females_45age_votes INT, top1000_voters_rating FLOAT, top1000_voters_votes INT, 
    us_voters_rating FLOAT); '''
    try:
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def insert_movies_votes_data():
    in_f = open("IMDb movies.csv", encoding="cp437", mode="r")
    csv_reader = csv.reader(in_f, delimiter=',')
    cmd = "INSERT INTO movies_votes (movie_id, avg_vote, votes, budget) VALUES (%s, %s, %s, %s)"
    skip = True
    i = 0
    for row in csv_reader:
        if skip:  # skip the title row.
            skip = False
        else:
            i += 1
            try:
                movie_id = int(row[0][2:])
                avg_vote = float(row[14])
                votes = int(row[15])
                budget_str = row[16]
                budget = 0
                if "$" in budget_str:
                    budget = ''.join((ch if ch in '0123456789' else '') for ch in budget_str)
                data = (movie_id, avg_vote, votes, budget)
                connector.execute_with_params(cmd, data)
                print(i)
            except Exception as e:
                print(row)
                print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def insert_movies_data():
    in_f = open("IMDb movies.csv", encoding="cp437", mode="r")
    csv_reader = csv.reader(in_f, delimiter=',')
    cmd = "INSERT INTO movies_votes (movie_id, avg_vote, votes, budget) VALUES (%s, %s, %s, %s)"
    skip = True
    i = 0
    for row in csv_reader:
        if skip:  # skip the title row.
            skip = False
        else:
            i += 1
            try:
                movie_id = int(row[0][2:])
                avg_vote = float(row[14])
                votes = int(row[15])
                budget_str = row[16]
                budget = 0
                if "$" in budget_str:
                    budget = ''.join((ch if ch in '0123456789' else '') for ch in budget_str)
                data = (movie_id, avg_vote, votes, budget)
                connector.execute_with_params(cmd, data)
                print(i)
            except Exception as e:
                print(row)
                print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def parse_birth_date(date_str):
    if len(date_str) == 0:
        return None
    if "c." in date_str:
        date_str = date_str.replace("c.", "").strip()
    if "BC" in date_str:
        date_str = date_str.replace("BC", "").strip()
    try:
        return datetime.fromisoformat(date_str)
    except:
        try:
            return datetime.strptime(date_str, '%B %d, %Y')
        except:
            try:
                return datetime.strptime(date_str, '%B, %Y')
            except:
                try:
                    return datetime.strptime(date_str, '%B %d')
                except:
                    return datetime.strptime(date_str, '%Y')


def parse_death_date(date_str):
    if len(date_str) == 0:
        return None
    date = date_str
    if "in" in date_str:
        date = date_str.split("in")[0].strip()
    return parse_birth_date(date)


def insert_actors_data():
    in_f = open("IMDb names.csv", encoding="cp437", mode="r")
    csv_reader = csv.reader(in_f, delimiter=',')

    cmd = "INSERT INTO actors (actor_id, name, birth_name, height, bio, birth_date, death_date, divorces, children) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
    max_len = 16000
    skip = True
    i = 0
    for row in csv_reader:
        if skip:  # skip the title row.
            skip = False
        else:
            i += 1
            try:
                actor_id = int(row[0][2:])
                name = row[1]
                birth_name = row[2]
                height = 0
                if not (len(row[3]) == 0):
                    height = int(row[3])
                bio = row[4]
                bio = bio[:min(len(bio), max_len)]
                birth_date = parse_birth_date(row[5].split("in")[0].strip())
                death_date = parse_death_date(row[6])
                divorces = int(row[14])
                children = int(row[16])
                data = (actor_id, name, birth_name, height, bio, birth_date, death_date, divorces, children)
                connector.execute_with_params(cmd, data)
                if i % 5000 == 0:
                    print(i)
            except Exception as e:
                print(row)
                print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def insert_ratings_data():
    in_f = open("IMDb ratings.csv", encoding="cp437", mode="r")
    csv_reader = csv.reader(in_f, delimiter=',')

    cmd = "INSERT INTO ratings (movie_id, weighted_average_vote, mean_vote, median_vote, " \
          "allgenders_0age_avg_vote, allgenders_0age_votes, allgenders_18age_avg_vote," \
          "allgenders_18age_votes ,allgenders_30age_avg_vote ,allgenders_30age_votes ," \
          "allgenders_45age_avg_vote ,allgenders_45age_votes ,males_allages_avg_vote ,males_allages_votes, " \
          "males_0age_avg_vote ,males_0age_votes ,males_18age_avg_vote ,males_18age_votes , " \
          "males_30age_avg_vote ,males_30age_votes ,males_45age_avg_vote ,males_45age_votes , " \
          "females_allages_avg_vote ,females_allages_votes ,females_0age_avg_vote ,females_0age_votes ," \
          "females_18age_avg_vote ,females_18age_votes ,females_30age_avg_vote ,females_30age_votes," \
          "females_45age_avg_vote ,females_45age_votes , top1000_voters_rating ,top1000_voters_votes," \
          "us_voters_rating) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    skip = True
    for mrow in csv_reader:
        if skip:  # skip the title row.
            skip = False
        else:
            movie_id = 0
            try:
                row = []
                for num in mrow:
                    x = num
                    if len(num) == 0:
                        x = "0"
                    row.append(x)

                movie_id = int(row[0][2:])
                weighted_average_vote = float(row[1])
                mean_vote = float(row[3])
                median_vote = float(row[4])
                allgenders_0age_avg_vote = float(row[15])
                allgenders_0age_votes = int(float(row[16]))
                allgenders_18age_avg_vote = float(row[17])
                allgenders_18age_votes = int(float(row[18]))
                allgenders_30age_avg_vote = float(row[19])
                allgenders_30age_votes = int(float(row[20]))
                allgenders_45age_avg_vote = float(row[21])
                allgenders_45age_votes = int(float(row[22]))
                males_allages_avg_vote = float(row[23])
                males_allages_votes = int(float(row[24]))
                males_0age_avg_vote = float(row[25])
                males_0age_votes = int(float(row[26]))
                males_18age_avg_vote = float(row[27])
                males_18age_votes = int(float(row[28]))
                males_30age_avg_vote = float(row[29])
                males_30age_votes = int(float(row[30]))
                males_45age_avg_vote = float(row[31])
                males_45age_votes = int(float(row[32]))
                females_allages_avg_vote = float(row[33])
                females_allages_votes = int(float(row[34]))
                females_0age_avg_vote = float(row[35])
                females_0age_votes = int(float(row[36]))
                females_18age_avg_vote = float(row[37])
                females_18age_votes = int(float(row[38]))
                females_30age_avg_vote = float(row[39])
                females_30age_votes = int(float(row[40]))
                females_45age_avg_vote = float(row[41])
                females_45age_votes = int(float(row[42]))
                top1000_voters_rating = float(row[43])
                top1000_voters_votes = int(float(row[44]))
                us_voters_rating = float(row[45])
                data = (movie_id, weighted_average_vote, mean_vote, median_vote, allgenders_0age_avg_vote, allgenders_0age_votes,
                        allgenders_18age_avg_vote, allgenders_18age_votes, allgenders_30age_avg_vote,
                        allgenders_30age_votes, allgenders_45age_avg_vote, allgenders_45age_votes,
                        males_allages_avg_vote, males_allages_votes,  males_0age_avg_vote,
                        males_0age_votes, males_18age_avg_vote, males_18age_votes, males_30age_avg_vote,
                        males_30age_votes, males_45age_avg_vote, males_45age_votes, females_allages_avg_vote,
                        females_allages_votes, females_0age_avg_vote, females_0age_votes, females_18age_avg_vote,
                        females_18age_votes, females_30age_avg_vote, females_30age_votes, females_45age_avg_vote,
                        females_45age_votes, top1000_voters_rating, top1000_voters_votes, us_voters_rating)
                connector.execute_with_params(cmd, data)
            except Exception as e:
                print(movie_id)
                print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def modifications():
    cmd = '''ALTER TABLE actors MODIFY bio VARCHAR(16000);'''
    connector.execute(cmd)


def insert_all_data():
    insert_movies_data()
    insert_movies_votes_data()
    insert_actors_data()
    insert_ratings_data()


if __name__ == "__main__":
    # create_db_tables()
    # insert_all_data()
    insert_ratings_data()
    # modifications()
    # connector.close()
