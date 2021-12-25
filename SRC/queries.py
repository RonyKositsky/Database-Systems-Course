import csv
from ModifiedConnector import ModifiedConnector

cursor = ModifiedConnector()


################      Top movies     ################
def query_1(num_of_votes, avg_vote):
    cmd = '''select m.title, mmv.votes, mmv.avg_votes
            from movies as m, 
            (select mv.votes as votes, mv.movie_id as movie_id, mv.avg_votes as avg_votes
            from movies_votes as mv
            where mv.votes >= %s and mv.avg_votes > %s) as mmv
            where m.movie_id = mmv.movie_id 
            order by mmv.avg_votes desc'''

    try:
        cursor.execute_with_params(cmd, (num_of_votes, avg_vote))
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    ret_data = cursor.fetch_data()
    #print(ret_data)
    for (title, votes, avg_votes) in ret_data:
        print("title : {}, votes: {}, avg_votes: {}".format(title, votes, avg_votes))

################      Top movies with at least X reviews     ################
def query_2(num_of_votes, avg_vote, num_of_reviews):
    cmd = '''select tm.title, mr.reviews_from_users
            from movies_reviews as mr, 
            (select m.title, mmv.votes, mmv.avg_votes, m.movie_id
            from movies as m, 
            (select mv.votes as votes, mv.movie_id as movie_id, mv.avg_votes as avg_votes
            from movies_votes as mv
            where mv.votes >= %s and mv.avg_votes > %s) as mmv
            where m.movie_id = mmv.movie_id 
            order by mmv.avg_votes desc ) as tm
            where tm.movie_id = mr.movie_id
            and mr.reviews_from_users > %s'''

    try:
        cursor.execute_with_params(cmd, (num_of_votes, avg_vote, num_of_reviews))
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    ret_data = cursor.fetch_data()
    #print(ret_data)
    for (title, reviews_from_users) in ret_data:
        print("title : {}, reviews_from_users: {}".format(title, reviews_from_users))

################      Counts number of movies of each genre (from top movies)     ################
def query_3(avg_vote):
    cmd = '''select mg.genres, count(*)
            from movies_genres mg,
            (select m.movie_id
            from movies_votes m
            where m.avg_votes > (%s)) as topm
            where mg.movie_id = topm.movie_id
            group by mg.genres'''%(avg_vote)

    try:
        #cursor.execute_with_params(cmd, (avg_vote))
        cursor.execute_query(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    ret_data = cursor.fetch_data()
    #print(ret_data)
    for (genre, count) in ret_data:
        print("genre : {}, count: {}".format(genre, count))

################      Actor and movie for the actors that are chosen by the user   ################
def query_4(list_of_actors_inp):
    list_of_actors = list_of_actors_inp.split(',')
    data = []
    cmd = '''select a.actor_name, a.title
                from
                (select a.actor_name, m.title
                from actors a, movies m, movies_productions mp
                WHERE MATCH(mp.actors) AGAINST (%s)
                and m.movie_id = mp.movie_id
                and  a.actor_name = %s) as a
                union
                select b.actor_name, b.title
                from
                (select a.actor_name, m.title
                from actors a, movies m, movies_productions mp
                WHERE MATCH(mp.actors) AGAINST (%s)
                and m.movie_id = mp.movie_id
                and  a.actor_name = %s) as b
                union
                select c.actor_name, c.title
                from
                (select a.actor_name, m.title
                from actors a, movies m, movies_productions mp
                WHERE MATCH(mp.actors) AGAINST (%s)
                and m.movie_id = mp.movie_id
                and  a.actor_name = %s) as c'''

    for actor in list_of_actors:
        data += [actor.strip(), actor.strip()]

    try:
        print(cmd, str(tuple(data)))
        cursor.execute_with_params(cmd, tuple(data))
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    ret_data = cursor.fetch_data()
    #print(ret_data)
    for (name, title) in ret_data:
        print("actor name : {}, title: {}".format(name, title))

################      Number of movies from each country and the average vote of them     ################
def query_5():
    cmd = '''select mc.country, count(mv.movie_id), avg(mv.votes)
            from movies_countries mc, movies_votes mv
            where mc.movie_id = mv.movie_id
            group by mc.country'''

    try:
        cursor.execute_query(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    ret_data = cursor.fetch_data()
    #print(ret_data)
    for (country, count, avg) in ret_data:
        print("country : {}, number of movies: {}, average vote: {}".format(country, count, avg))

################      Union of all the movies of 3 categories chosen by the user   ################
def query_6(list_of_categories_inp):
    list_of_categories = list_of_categories_inp.split(',')
    cmd = '''select mc.title 
            from (select m.title
            from movies m, movies_productions mp
            WHERE MATCH(mp.description) AGAINST (%s)) as mc
            union 
            select mr.title
            from (select m.title
            from movies m, movies_productions mp
            WHERE MATCH(mp.description) AGAINST (%s)) as mr
            union 
            select mk.title
            from (select m.title
            from movies m, movies_productions mp
            WHERE MATCH(mp.description) AGAINST (%s)) as mk'''

    try:
        print(cmd, str(tuple(list_of_categories)))
        cursor.execute_with_params(cmd, tuple(list_of_categories))
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    ret_data = cursor.fetch_data()
    #print(ret_data)
    for (title) in ret_data:
        print("title: {}".format(title))

################      Table with countries total movies budget and avg vote of them     ################
def query_7():
    cmd = '''select  mc.country, sum(mb.budget) as total_budget, avg(mv.avg_votes) as avg_country_votes
            from movies_countries mc, movies_budget mb, movies_votes mv
            where mc.movie_id = mb.movie_id
            and mc.movie_id = mv.movie_id
            group by mc.country
            having sum(mb.budget) > 0
            order by total_budget, avg_country_votes desc'''

    try:
        cursor.execute_query(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    ret_data = cursor.fetch_data()
    #print(ret_data)
    for (country, total_budget, avg_country_votes) in ret_data:
        print("country : {}, total_budget: {}, average vote: {}".format(country, total_budget, avg_country_votes))

################      Top rated movies by men union 100 top rated movies by womens     ################
def query_8():
    cmd = '''select f.title, f.avg_vote
            from
            (select m.title, r.female_avg as avg_vote
            from ratings r, movies m
            where r.movie_id = m.movie_id
            order by r.female_avg desc
            limit 100) as f
            union
            select m.title, m.avg_vote
            from
            (select m.title, r.male_avg as avg_vote
            from ratings r, movies m
            where r.movie_id = m.movie_id
            order by r.male_avg desc
            limit 100) as m'''

    try:
        cursor.execute_query(cmd)
    except Exception as e:
        print(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    ret_data = cursor.fetch_data()
    #print(ret_data)
    for (title, avg_vote) in ret_data:
        print("title : {}, avg_vote: {}".format(title, avg_vote))


if __name__ == "__main__":
    print('Execute Query No. 1 : Top movies')
    num_of_votes = input("Please enter the minimum number of votes: ")
    avg_vote = input("Please enter the minimum avg_vote: ")
    query_1(num_of_votes, avg_vote)

    print('Execute Query No. 2 : Top movies with at least X reviews')
    num_of_votes = input("Please enter the minimum number of votes: ")
    avg_vote = input("Please enter the minimum avg_vote: ")
    num_of_reviews = input("Please enter the minimum number of reviews: ")
    query_2(num_of_votes, avg_vote, num_of_reviews)

    print('Execute Query No. 3 : Counts number of movies of each genre (from top movies)')
    avg_vote = input("Please enter the minimum avg_vote: ")
    query_3()

    print('Execute Query No. 4 : Actor and movie for the actors that are chosen by the user')
    list_of_actors_inp = input("Please enter a list of 3 actors: ")
    query_4()

    print('Execute Query No. 5 : Number of movies from each country and the average vote of them')
    query_5()

    print('Execute Query No. 6 : Union of all the movies of 3 categories chosen by the user')
    list_of_categories_inp = input("Please enter a list of 3 movies categories: ")
    query_6(list_of_categories_inp)

    print('Execute Query No. 7 : Table with countries total movies budget and avg vote of them')
    query_7()

    print('Execute Query No. 8 : Top rated movies by men union 100 top rated movies by womens')
    query_8()