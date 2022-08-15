from flask import Flask, request
from services import movie_service, user_service
from security.authorization import token_required

app = Flask(__name__)


@app.route('/')
@token_required
def hello(user):
    return movie_service.hello()


@app.route('/movie/createTable')
@token_required
def create_table():
    return movie_service.create_table_movies()


@app.route('/movie/loadData')
@token_required
def load_data_to_db():
    return movie_service.load_data()


@app.route('/readData/<string:id>', methods=['GET'])
@token_required
def get_data(id):
    return movie_service.read_from_movies(id)


@app.route('/deleteMovie/<string:id>', methods=['DELETE'])
@token_required
def delete_movie(id):
    return movie_service.delete_from_movie(id)


@app.route('/updateMovie/<string:id>', methods=['PUT'])
@token_required
def update_movie(id):
    data = request.get_json()
    return movie_service.update_in_movie(id, data)


@app.route('/dir/getReport', methods=['POST'])
@token_required
def get_report_dir():
    data = request.get_json()
    return movie_service.get_report_dir(data)


@app.route('/review/getReport', methods=['POST'])
@token_required
def get_report_review():
    data = request.get_json()
    return movie_service.get_report_review(data)


@app.route('/user/createTable')
@token_required
def create_user_table():
    return user_service.create_user_table();


@app.route('/user/login', methods=['POST'])
def login():
    data = request.get_json()
    return user_service.login(data);


@app.route('/budgetReport', methods=['POST'])
def budget_report():
    data = request.get_json()
    return movie_service.budget_report(data)

if __name__ == '__main__':
    app.run(port=4000);
