import os
import logging

from dotenv import load_dotenv
from flask import Flask, jsonify, request

logging.basicConfig(level='INFO')

load_dotenv()

genres = {
    1: {'id': 1, 'name': 'Action'},
    2: {'id': 2, 'name': 'Comedy'},
    3: {'id': 3, 'name': 'Romantic'},
}

movies = {
    1: {'id': 1, 'name': 'Mission Impossible', 'release_date': '1996-05-22'},
    2: {'id': 2, 'name': 'Scary Movie', 'release_date': '2000-07-07'},
    3: {'id': 3, 'name': 'Notting Hill', 'release_date': '1999-05-21'},
    4: {'id': 4, 'name': 'Die Hard', 'release_date': '1988-12-22'},
}

genre_movies = {
    1: {'id': 1, 'id_genre': 1, 'id_movie': 1},
    2: {'id': 2, 'id_genre': 2, 'id_movie': 2},
    3: {'id': 3, 'id_genre': 3, 'id_movie': 3},
    4: {'id': 4, 'id_genre': 1, 'id_movie': 4},
}

movies_ratings = {
    1: {'id': 1, 'id_movie': 1, 'rating': 3},
    2: {'id': 2, 'id_movie': 2, 'rating': 5},
    3: {'id': 3, 'id_movie': 3, 'rating': 4},
    4: {'id': 4, 'id_movie': 1, 'rating': 5},
    5: {'id': 5, 'id_movie': 2, 'rating': 4},
    6: {'id': 6, 'id_movie': 3, 'rating': 4},
}


app = Flask(__name__)


@app.route('/auth', methods=['POST'])
def authenticate():
    data = request.get_json() or {}
    username = data.get('username')
    password = data.get('password')

    # Validação simples de credenciais simuladas
    if username == os.getenv('USERNAME') and password == os.getenv('PASSWORD'):
        logging.info('AUTHENTICATION SUCCESS')
        return jsonify({'access_token': '123456', 'token_type': 'Bearer'}), 200

    # Retorna erro caso as credenciais estejam erradas
    logging.error('AUTHENTICATION FAIL')
    return jsonify({'error': 'Unauthorized', 'message': 'Credenciais inválidas'}), 401


@app.route('/obras/v3/generos', methods=['GET'])
def list_genres():

    all_genres = list(genres.values())

    return jsonify(all_genres), 200


# Simulate a GET request endpoint
@app.route('/obras/v3/generos/<int:idGenero>/filmes', methods=['GET'])
def get_genre_movies(idGenero: int):
    movies = [movie for movie in genre_movies.values() if movie['id_genre'] == idGenero]

    return jsonify(movies), 200


@app.route('/obras/v3/filmes/<int:idFilme>', methods=['GET'])
def get_movie(idFilme: int):
    movie_data = [movie for movie in movies.values() if movie['id'] == idFilme]

    if movie_data:
        return jsonify(movie_data[0]), 200
    return jsonify({'error': 'Movie not found'}), 404


@app.route('/obras/v3/filmes/<int:idFilme>/avaliacoes', methods=['GET'])
def get_movie_ratings(idFilme: int):
    ratings = [
        movie for movie in movies_ratings.values() if movie['id_movie'] == idFilme
    ]

    return jsonify(ratings), 200


if __name__ == '__main__':
    # Start local server on http://127.0.0.1:5000
    app.run(debug=True, port=5000)
