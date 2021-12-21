from tmdbv3api import TMDb
from tmdbv3api import Discover
from tmdbv3api import Genre
from SRC.ModifiedConnector import ModifiedConnector

connector = ModifiedConnector()

# Open connection to API.
tmdb = TMDb()
tmdb.api_key = '58a785782ea501b2d0684e158f09ad4b'
tmdb.language = 'en'
tmdb.debug = True

# Get all the API movies.
discover = Discover()
genre = Genre()
movies = discover.discover_movies({})
genres_dict = genre.movie_list()

def import_to_tables(dict):
    cmds = [
        "INSERT INTO movies (movie_id, title) VALUES (%s, %s)",
        "INSERT INTO movies_countries (movie_id, language) VALUES (%s, %s)",
        "INSERT INTO movies_votes (movie_id, avg_votes, votes) VALUES (%s, %s, %s)",
        "INSERT INTO movies_genres (movie_id, genres) VALUES (%s, %s)"
    ]
    datas = [
        (dict["id"], dict["title"]),
        (dict["id"], dict["language"]),
        (dict["id"], dict["vote_avg"], dict["votes"]),
        (dict["id"], dict["genre"])
    ]

    for i in range(len(cmds)):
        try:
            connector.execute_with_params(cmds[i], datas[i])
        except Exception as e:
            print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")


def parse_movie(movie_obj):
    params_dict = {
        "id": movie_obj["id"],
        "title": movie_obj["title"],
        "language": movie_obj["original_language"],
        "vote_avg": movie_obj["vote_average"],
        "votes": movie_obj["vote_count"]}
    genre_str = ""
    genres_params = movie_obj["genre_ids"]
    for genre_id in genres_params:
        for element in genres_dict:
            if genre_id == element["id"]:
                genre_str += element["name"] + ","
                break
    genre_str = genre_str[:-1]
    params_dict["genre"] = genre_str

    import_to_tables(params_dict)


if __name__ == "__main__":
    for movie in movies:
        parse_movie(movie)
