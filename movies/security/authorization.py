from functools import wraps

from boto3.dynamodb.conditions import Attr
from flask import request, jsonify
import jwt
import config
from boto3 import resource

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

userTable = resource.Table('users')


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']
        if not token:
            return {'message': 'Token is missing!'}, 401
        try:
            data = jwt.decode(token, config.SECRET_KEY)
            current_user = userTable.get_item(Key={'username': data['username']},
                                              AttributesToGet=['username', 'password']
                                              )
        except:
            return {'message': 'Token is invalid!'}, 401

        return f(*args, **kwargs)

    return decorated
