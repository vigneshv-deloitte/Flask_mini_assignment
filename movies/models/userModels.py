from werkzeug.security import generate_password_hash


def create_table_users(resource):
    user_table = resource.Table('users')
    resource.create_table(
        TableName='users',  # Name of the table
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'  # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',  # Name of the attribute
                'AttributeType': 'S'  # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    response = user_table.put_item(
        Item={
            'username': 'admin',
            'password': generate_password_hash('admin', method='sha256')
        }
    )

    return {'message': 'Table created'}
