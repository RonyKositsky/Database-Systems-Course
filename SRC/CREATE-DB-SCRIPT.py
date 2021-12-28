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
    cmds = ['CREATE TABLE movies (movie_id INT, title VARCHAR(1000), movie_year INT, PRIMARY KEY(movie_id));',
            'CREATE TABLE movies_genres (movie_id INT, genres VARCHAR(1000) , PRIMARY KEY(movie_id));',
            'CREATE TABLE movies_countries (movie_id INT, country VARCHAR(1000), language VARCHAR(1000), PRIMARY KEY('
            'movie_id));',
            'CREATE TABLE movies_productions (movie_id INT, director VARCHAR(1000), writer VARCHAR(1000), productions '
            'VARCHAR(1000), actors VARCHAR(1000), description VARCHAR(1000) , PRIMARY KEY(movie_id), FULLTEXT(actors),'
            ' FULLTEXT(description));',
            'CREATE TABLE movies_votes (movie_id INT, avg_votes FLOAT, votes INT, critics_votes INT , PRIMARY KEY('
            'movie_id));',
            'CREATE TABLE movies_budget (movie_id INT, budget FLOAT , PRIMARY KEY(movie_id));',
            'CREATE TABLE actors (actor_id INT, actor_name VARCHAR(1000), bio VARCHAR(10000) , PRIMARY KEY(actor_id), '
            'FULLTEXT(actor_name), FULLTEXT(bio));',
            'CREATE TABLE ratings (movie_id INT, male_avg FLOAT, female_avg FLOAT , PRIMARY KEY(movie_id));',
            'CREATE TABLE movies_reviews (movie_id INT, reviews_from_users INT, reviews_from_critics INT);',
            'CREATE INDEX actor_index ON actors (actor_name);',
            'CREATE INDEX votes_index ON movies_votes (votes);']

    for cmd in cmds:
        create_table(cmd)


if __name__ == "__main__":
    create_db_tables()
    connector.close()
