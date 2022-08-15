from werkzeug.security import check_password_hash
from models.userModels import create_table_users
from flask import request, make_response, jsonify
from boto3 import resource
import config, jwt, datetime

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

UserTable = resource.Table('users')


def create_user_table():
    return create_table_users(resource);


def login(data):
    try:
        username = data['username']
        password = data['password']

    except:
        username = None
        password = None

    if not username or not password:
        return make_response('Please provide both Username and Password', 400,
                             {'WWW-Authenticate': 'Basic realm="Login required!"'})

    response = UserTable.get_item(
        Key={
            'username': username
        },
        AttributesToGet=['username', 'password']
    )
    if 'Item' in response:
        if check_password_hash(response['Item']['password'], password):
            token = jwt.encode({'username': response['Item']['username'],
                                'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=120)}, config.SECRET_KEY)
            return jsonify({'token': token.decode('UTF-8')})
        else:
            return make_response('Could not verify, Incorrect username or password', 401,
                                 {'WWW-Authenticate': 'Basic realm="Login required!"'})
            return msg_dict
    return make_response("User doesn't exists", 404,
                         {'WWW-Authenticate': 'Basic realm="Login required!"'})
