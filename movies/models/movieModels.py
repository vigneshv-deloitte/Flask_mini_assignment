def create_table_movies(resource):
    resource.Table('movies').delete()
    table = resource.create_table(
        TableName='movies',  # Name of the table
        KeySchema=[
            {
                'AttributeName': 'imdb_title_id',
                'KeyType': 'HASH'  # HASH = partition key, RANGE = sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'imdb_title_id',  # Name of the attribute
                'AttributeType': 'S'  # N = Number (S = String, B= Binary)
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 1000,
            'WriteCapacityUnits': 1000
        }
    )
    return {'message': 'Table created'}
