from boto3 import resource
from boto3.dynamodb.conditions import Attr
import config, time, re, logging
from functools import wraps
from models.movieModels import create_table_movies as create_movie_table

# logging.basicConfig()
# logging.root.setLevel(logging.INFO)
logging.basicConfig(filename='info.log',level=logging.INFO)


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        response = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        response['X-TIME-TO-EXECUTE'] = total_time
        return response

    return timeit_wrapper


AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)


@timeit
def hello():
    return {'message': 'hello'}


MoviesTable = resource.Table('movies')


@timeit
def create_table_movies():
    logging.info("Function create_table_movies was called")
    return create_movie_table(resource)


def write_to_movies(imdb_title_id,
                    title, original_title, year, date_published, genre, duration, country, language,
                    director, writer, production_company, actors, description, avg_vote, votes, budget,
                    usa_gross_income, worlwide_gross_income, metascore, reviews_from_users, reviews_from_critics
                    ):
    logging.info("Function write_to_movies was called")
    try:
        response = MoviesTable.put_item(
            Item={
                'imdb_title_id': imdb_title_id,
                'title': title,
                'original_title': original_title,
                'year': year,
                'date_published': date_published,
                'genre': genre,
                'duration': duration,
                'country': country,
                'language': language,
                'director': director,
                'writer': writer,
                'production_company': production_company,
                'actors': actors,
                'description': description,
                'avg_vote': avg_vote,
                'votes': votes,
                'budget': budget,
                'usa_gross_income': usa_gross_income,
                'worlwide_gross_income': worlwide_gross_income,
                'metascore': metascore,
                'reviews_from_users': reviews_from_users,
                'reviews_from_critics': reviews_from_critics
            }
        )
    except:
        logging.error("Found error while adding item with imdb_title_id: %s" % imdb_title_id)

    return response


@timeit
def load_data():
    logging.info("Function load_data was called")
    total = 0
    exp = ',(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))'
    with open('resources/movies.csv', 'r') as movies:
        rows = [row.rstrip() for row in movies]

        count = 0
        for row in rows:
            words = re.split(exp, row)

            if total != 0:
                if words[20]:
                    review = int(words[20])
                else:
                    review = 0
                if words[16]:
                    budget = int(''.join(filter(lambda x: x.isdigit(), words[16])))
                else:
                    budget = 0
                if words[7]:
                    country = ''.join(words[7].split(' ')).capitalize()
                else:
                    country = None
                response = write_to_movies(words[0], words[1],
                                           words[2], words[3], words[4], words[5], words[6], country,
                                           words[8], words[9],
                                           words[10]
                                           , words[11], words[12], words[13], words[14], words[15], budget,
                                           words[17],
                                           words[18],
                                           words[19], review, words[21]
                                           )

                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    pass
                else:
                    logging.error("Error occurred!")
                count += 1

            total += 1
    return {'message': 'Table loaded'}


@timeit
def read_from_movies(imdb_title_id):
    logging.info("Function read_from_movies was called")

    response = MoviesTable.get_item(
        Key={
            'imdb_title_id': imdb_title_id
        },
        AttributesToGet=["title", "original_title", "year", "date_published", "genre", "duration",
                         "country", "language",
                         "director", "writer", "production_company", "actors", "description", "avg_vote", "votes",
                         "budget",
                         "usa_gross_income", "worlwide_gross_income", "metascore", "reviews_from_users",
                         "reviews_from_critics"]
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        if 'Item' in response:
            return {'Item': response['Item']}
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }


@timeit
def delete_from_movie(imdb_title_id):
    logging.info("Function delete_from_movie was called")

    response = MoviesTable.delete_item(
        Key={
            'imdb_title_id': imdb_title_id
        }
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return {
            'msg': 'Deleted successfully',
        }
    return {
        'msg': 'Some error occcured',
        'response': response
    }


def update_in_movie(imdb_title_id, data: dict):
    logging.info("Function update_in_movie was called")

    response = MoviesTable.update_item(
        Key={
            'imdb_title_id': imdb_title_id
        },
        AttributeUpdates={

            'title': {
                'Value': data['title'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },

            'original_title': {
                'Value': data['original_title'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },

            'year': {
                'Value': data['year'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },

            'date_published': {
                'Value': data['date_published'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'genre': {
                'Value': data['genre'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'duration': {
                'Value': data['duration'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'country': {
                'Value': data['country'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'language': {
                'Value': data['language'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'director': {
                'Value': data['director'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'writer': {
                'Value': data['writer'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'production_company': {
                'Value': data['production_company'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },

            'actors': {
                'Value': data['actors'],
                'Action': 'PUT'
            },
            'description': {
                'Value': data['description'],
                'Action': 'PUT'
            },
            'avg_vote': {
                'Value': data['avg_vote'],
                'Action': 'PUT'
            },
            'votes': {
                'Value': data['votes'],
                'Action': 'PUT'
            },
            'usa_gross_income': {
                'Value': data['usa_gross_income'],
                'Action': 'PUT'
            },
            'budget': {
                'Value': data['budget'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'worlwide_gross_income': {
                'Value': data['worlwide_gross_income'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'metascore': {
                'Value': data['metascore'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'reviews_from_users': {
                'Value': data['reviews_from_users'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },
            'reviews_from_critics': {
                'Value': data['reviews_from_critics'],
                'Action': 'PUT'  # # available options = DELETE(delete), PUT(set/update), ADD(increment)
            },

        },
        ReturnValues="UPDATED_NEW"  # returns the new updated values
    )
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        return {
            'msg': 'Updated successfully',
            'ModifiedAttributes': response['Attributes'],
            'response': response['ResponseMetadata']
        }
    return {
        'msg': 'Some error occured',
        'response': response
    }


@timeit
def get_report_dir(data: dict):
    logging.info("Function get_report_dir was called")

    director = data['director']
    start = data['from']
    end = data['to']
    response = MoviesTable.scan(
        FilterExpression=Attr('director').eq(director) & Attr('year').gte(start) & Attr('year').lte(end))

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        result_list = []
        for items in response["Items"]:
            result_list.append({'title': items['title'], 'year': items['year']})
        return {'Report': result_list}

    return {
        'msg': 'Some error occured',
        'response': response['Items']
    }


@timeit
def get_report_review(data: dict):
    logging.info("Function get_report_review was called")

    review = data['reviews_from_users']

    response = MoviesTable.scan(FilterExpression=Attr('reviews_from_users').gt(review))

    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        d = response['Items']
        sorted_list = sorted(d, key=lambda x: x['reviews_from_users'], reverse=True)
        result_list = []

        for items in sorted_list:
            result_list.append({'title': items['title'], 'reviews_from_users': items['reviews_from_users']})
        return {'Report': result_list}
    return {
        'msg': 'Some error occured',
        'response': response['Items']
    }


@timeit
def budget_report(data):
    year = data['year']
    country = data['country'].capitalize()

    response = MoviesTable.scan(FilterExpression=Attr('year').eq(year) & Attr('country').eq(country))
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        d = response['Items']

        sorted_list = sorted(d, key=lambda x: x['budget'], reverse=True)
        result_list = []

        for items in sorted_list:
            result_list.append({'title': items['title'], 'budget': items['budget'], 'year': items['year'],
                                'country': items['country']})
        return {'Report': result_list}
    return {
        'msg': 'Some error occured',
        'response': response['Items']
    }
