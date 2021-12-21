import csv
from ModifiedConnector import ModifiedConnector

connector = ModifiedConnector()


def create_table(cmd):
    try:
        connector.execute(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def create_db_tables():
    """
        Creating the tables in our database.
        First, it will delete any possible existing table, then it will create them.
    """
    cmds = ['CREATE TABLE movies (movie_id INT, title VARCHAR(1000), year INT);',
            'CREATE TABLE movies_genres (movie_id INT, genres VARCHAR(1000));',
            'CREATE TABLE movies_countries (movie_id INT, country VARCHAR(1000), language VARCHAR(1000));',
            'CREATE TABLE movies_productions (movie_id INT, director VARCHAR(1000), writer VARCHAR(1000), productions '
            'VARCHAR(1000), actors VARCHAR(1000), description VARCHAR(1000));',
            'CREATE TABLE movies_votes (movie_id INT, avg_votes FLOAT, votes INT, critics_votes INT);',
            'CREATE TABLE movies_budget (movie_id INT, budget FLOAT);',
            'CREATE TABLE actors (actor_id INT, name VARCHAR(1000), bio VARCHAR(10000));',
            'CREATE TABLE actors_birth (actor_id INT, place_of_birth VARCHAR(1000));',
            'CREATE TABLE ratings (movie_id INT, male_avg FLOAT, female_avg FLOAT);']

    for cmd in cmds:
        create_table(cmd)


def open_csv_reader(csv_file_name):
    in_f = open(f"{csv_file_name}", encoding="cp437", mode="r")
    return csv.reader(in_f, delimiter=',')


def insert_movies_data():
    csv_reader = open_csv_reader("IMDb movies.csv")
    cmd = "INSERT INTO movies (movie_id, title, year) VALUES (%s, %s, %s)"

    for row in list(csv_reader)[1:]:
        try:
            movie_id = int(row[0][2:])
            title = row[1]
            year = int(''.join((ch if ch in '0123456789' else '') for ch in row[3]))
            data = (movie_id, title, year)
            connector.execute_with_params(cmd, data)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    print("Finished")


def insert_movies_genres_data():
    csv_reader = open_csv_reader("IMDb movies.csv")
    cmd = "INSERT INTO movies_genres (movie_id, genres) VALUES (%s, %s)"
    for row in list(csv_reader)[1:]:
        try:
            movie_id = int(row[0][2:])
            genre = row[5]
            data = (movie_id, genre)
            connector.execute_with_params(cmd, data)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    print("Finished")


def insert_movies_countries_data():
    csv_reader = open_csv_reader("IMDb movies.csv")
    cmd = "INSERT INTO movies_countries (movie_id, country, language) VALUES (%s, %s, %s)"
    for row in list(csv_reader)[1:]:
        try:
            movie_id = int(row[0][2:])
            country = row[7]
            language = row[8]
            if language.strip() == "None" or language.strip() == "":
                language = None
            data = (movie_id, country, language)
            connector.execute_with_params(cmd, data)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    print("Finished")


def insert_movies_productions_data():
    csv_reader = open_csv_reader("IMDb movies.csv")
    cmd = "INSERT INTO movies_productions (movie_id, director, writer, productions, actors, description)" \
          " VALUES (%s, %s, %s, %s, %s, %s)"
    for row in list(csv_reader)[1:]:
        try:
            movie_id = int(row[0][2:])
            director = row[9]
            writer = row[10]
            productions = row[11]
            actors = row[12]
            description = row[13]
            data = (movie_id, director, writer, productions, actors, description)
            connector.execute_with_params(cmd, data)

        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    print("Finished")


def insert_movies_votes_data():
    csv_reader = open_csv_reader("IMDb movies.csv")
    cmd = "INSERT INTO movies_votes (movie_id, avg_votes, votes, critics_votes) VALUES (%s, %s, %s, %s)"

    for row in list(csv_reader)[1:]:
        try:
            movie_id = int(row[0][2:])
            avg_votes = float(row[14])
            votes = int(row[15])
            critics_votes = 0
            if row[21] != "":
                critics_votes = float(row[21])
            data = (movie_id, avg_votes, votes, critics_votes)
            connector.execute_with_params(cmd, data)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    print("Finished")


def insert_movies_budget_data():
    csv_reader = open_csv_reader("IMDb movies.csv")
    cmd = "INSERT INTO movies_budget (movie_id, budget) VALUES (%s, %s)"
    for row in list(csv_reader)[1:]:
        try:
            movie_id = int(row[0][2:])
            budget_str = row[16]
            budget = 0
            if "$" in budget_str:
                budget = float(''.join((ch if ch in '0123456789' else '') for ch in budget_str))
            data = (movie_id, budget)
            connector.execute_with_params(cmd, data)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    print("Finished")


def insert_actors_data():
    csv_reader = open_csv_reader("IMDb names.csv")
    cmd = "INSERT INTO actors (actor_id, name, bio) VALUES (%s, %s, %s)"
    MAX_SIZE = 15000

    for row in list(csv_reader)[1:]:
        try:
            actor_id = int(row[0][2:])
            name = row[1]
            bio = row[4]
            if len(bio) > MAX_SIZE:
                bio = bio[:MAX_SIZE]
            data = (actor_id, name, bio)
            connector.execute_with_params(cmd, data)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    print("Finished")


def insert_ratings_data():
    csv_reader = open_csv_reader("IMDb ratings.csv")
    cmd = "INSERT INTO ratings (movie_id, male_avg, female_avg) VALUES (%s, %s, %s)"
    for row in list(csv_reader)[1:]:
        try:
            movie_id = int(row[0][2:])
            male_avg = None
            if row[23] != '':
                male_avg = float(row[23])
            female_avg = None
            if row[33] != '':
                female_avg = float(row[33])
            data = (movie_id, male_avg, female_avg)
            connector.execute_with_params(cmd, data)
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
    print("Finished")


def insert_all_data():
    insert_movies_data()
    insert_movies_genres_data()
    insert_movies_countries_data()
    insert_movies_productions_data()
    insert_movies_votes_data()
    insert_movies_budget_data()
    insert_actors_data()
    insert_ratings_data()


def modifications():
    cmd = '''ALTER TABLE actors MODIFY bio VARCHAR(15000);'''
    connector.execute(cmd)


if __name__ == "__main__":
    create_db_tables()
    insert_all_data()
    insert_movies_data()
    # modifications()
    connector.close()
