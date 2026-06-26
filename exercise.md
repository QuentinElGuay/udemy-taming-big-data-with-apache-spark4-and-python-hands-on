**Important:** I formated this document as markdown. For a better readability, open it in a markdown reader.

I decided to use Pandas which handles data in memory because I perceived that the endpoints are not very efficent for a database dump. It is then my understanding that the volume of data is rather small and, in consequence. If the volume where to increase over the memory capacity of the machine, I would recommend another strategy with pagination, paralelization, buffer to write on the disk or in a datalake regularly, more efficient libraries for a high volume of data like Polars or DuckDB.

Also, because the instructions are to create a table then populate, I decided to implement a `full-refresh` strategy instead of an incremental one.

Finaly, it is my understanding that only one table was expected. This is why I opted for a One Big Table strategy. Another strategy could have been to use an ELT strategy to populate dimensions (movies, genre) and facts (genre_movie, movie_rating) tables to only later join data in a final table. This would allow to preserve all ratings instead of aggregating them.

To avoid handling too many limit cases, I used the following premises:
- the endpoints do not return duplicated objects
- there can be only one genre by movie
- there is at least one rating to be returned by the `avaliacoes` endpoint

Content of my fake API:
```json
{
    "genres": [
        {"id": 1, "name": 'Action'},
        {"id": 2, "name": 'Comedy'},
        {"id": 3, "name": 'Romantic'}
    ],
    "movies": [
        {"id": 1, "name": 'Mission Impossible', "release_date": '1996-05-22'},
        {"id": 2, "name": 'Scary Movie', "release_date": '2000-07-07'},
        {"id": 3, "name": 'Notting Hill', "release_date": '1999-05-21'},
        {"id": 4, "name": 'Die Hard', "release_date": '1988-12-22'}
    ],
    "genre_movies":[
        {"id": 1, "id_genre": 1, "id_movie": 1},
        {"id": 2, "id_genre": 2, "id_movie": 2},
        {"id": 3, "id_genre": 3, "id_movie": 3},
        {"id": 4, "id_genre": 1, "id_movie": 4}
    ],
    "movies_ratings": [
        {"id": 1, "id_movie": 1, "rating": 3},
        {"id": 2, "id_movie": 2, "rating": 5},
        {"id": 3, "id_movie": 3, "rating": 4},
        {"id": 4, "id_movie": 1, "rating": 5},
        {"id": 5, "id_movie": 2, "rating": 4},
        {"id": 6, "id_movie": 3, "rating": 4}
    ]
}
```

Python ETL code:
```python
from dataclasses import dataclass
import logging
import os
import sys
from typing import Any
from urllib.parse import urljoin
from urllib3.util.retry import Retry

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine.base import Engine
from dotenv import load_dotenv
import pandas as pd
import requests
from requests.adapters import HTTPAdapter

load_dotenv()

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(
    logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
)

logger = logging.getLogger('ETL')
logger.setLevel(os.getenv('LOG_LEVEL', 'INFO').upper())
logger.addHandler(handler)
logger.propagate = False


API_BASE_URL = os.environ['API_BASE_URL']
AUTH_ENDPOINT = '/auth'
GENRES_ENDPOINT = '/obras/v3/generos'
GENRE_MOVIES_ENDPOINT = '/obras/v3/generos/{idGenero}/filmes'
MOVIES_ENDPOINT = '/obras/v3/filmes/{idFilme}'
MOVIE_RATINGS_ENDPOINT = '/obras/v3/filmes/{idFilme}/avaliacoes'


@dataclass
class PostgresConnection:
    """
    Information required to connect to a Postgres instances
    """

    user: str
    password: str
    database: str
    host: str
    port: str

    @property
    def url(self) -> str:
        return (
            'postgresql+psycopg2://'
            f'{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        )


def get_auth(
    session: requests.Session, url: str, username: str, password: str, timeout: int = 5
) -> str:
    """
    Returns a token from an basic authentication endpoint
    """
    response = session.post(
        url, json={'username': username, 'password': password}, timeout=timeout
    )

    try:
        response.raise_for_status()
        logger.debug('Successul authentication to API.')

    except Exception as error:
        logger.error('Unable to authenticate to API: %s.', error)
        raise

    return response.json()['access_token']


def get_endpoint(session: requests.Session, url: str, timeout: int = 5) -> Any:
    """
    Returns the result of a GET call to an endpoint
    """
    response = session.get(url, timeout=timeout)

    try:
        response.raise_for_status()

    except Exception as error:
        logger.error('Unable to retrieve endpoint %s: %s', url, error)
        raise

    return response.json()


def get_genres(session: requests.Session) -> list[dict]:
    """
    Returns a list of genres returned by the API endpoints.
    """
    genres = get_endpoint(session, urljoin(API_BASE_URL, GENRES_ENDPOINT))
    logger.info('Downloaded %d genres from endpoint.', len(genres))
    return genres


def get_genre_movies(session: requests.Session, id_genres: list[dict]) -> list[dict]:
    """
    Returns a list of relations genre/movies returned by the API endpoints.
    """
    genre_movies = [
        movie
        for id_genre in id_genres
        for movie in get_endpoint(
            session,
            urljoin(API_BASE_URL, GENRE_MOVIES_ENDPOINT.format(idGenero=id_genre)),
        )
    ]

    logger.info('Downloaded %d genre_movies from endpoint.', len(genre_movies))

    return genre_movies


def get_movies(session: requests.Session, id_movies: list[dict]) -> list[dict]:
    """
    Returns a list of relations movies returned by the API endpoints.
    """
    movies = [
        get_endpoint(
            session,
            urljoin(API_BASE_URL, MOVIES_ENDPOINT.format(idFilme=id_movie)),
        )
        for id_movie in id_movies
    ]
    logger.info('Downloaded %d movies from endpoint.', len(movies))

    return movies


def get_movie_ratings(session: requests.Session, id_movies: list[str]) -> list[dict]:
    """
    Returns a list of relations movie/ratings returned by the API endpoints.
    """
    movie_ratings = [
        rating
        for id_movie in id_movies
        for rating in get_endpoint(
            session,
            urljoin(API_BASE_URL, MOVIE_RATINGS_ENDPOINT.format(idFilme=id_movie)),
        )
    ]

    logger.info(
        'Downloaded %d ratings from endpoint.',
        len(movie_ratings),
    )

    return movie_ratings


def extract() -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """
    Execute the Extract step of the ETL process
    """

    logger.info('- STARTING EXTRACT STEP -')

    with requests.Session() as session:
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=['GET', 'POST'],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('http://', adapter)
        session.mount('https://', adapter)

        token = get_auth(
            session,
            urljoin(API_BASE_URL, AUTH_ENDPOINT),
            os.environ['USERNAME'],
            os.environ['PASSWORD'],
        )

        headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

        session.headers.update(headers)

        genres = get_genres(session)

        id_genres = [genre['id'] for genre in genres]
        genre_movies = get_genre_movies(session, id_genres)

        id_movies = [gm['id_movie'] for gm in genre_movies]
        movies = get_movies(session, id_movies)
        movie_ratings = get_movie_ratings(session, id_movies)

    logger.info('- EXTRACT STEP EXECUTED WITH SUCCESS -')

    return genres, movies, genre_movies, movie_ratings


def transform(
    genres: list[dict],
    movies: list[dict],
    genres_movies: list[dict],
    movies_ratings: list[dict],
) -> pd.DataFrame:
    """
    Execute the Transform step of the ETL process
    """
    logger.info('- STARTING TRANSFORM STEP -')

    df_genres = pd.DataFrame(genres)
    df_genres.rename(columns={'name': 'genre'}, inplace=True)
    df_movies = pd.DataFrame(movies)
    df_genres_movies = pd.DataFrame(genres_movies)
    df_movies_ratings = pd.DataFrame(movies_ratings)

    df_aggregations = df_movies_ratings.groupby(['id_movie'], as_index=False).agg(
        qty_ratings=('rating', 'count'),
        avg_rating=('rating', 'mean'),
        min_rating=('rating', 'min'),
        max_rating=('rating', 'max'),
    )

    df_exportation = (
        df_genres.merge(df_genres_movies, left_on='id', right_on='id_genre')
        .drop(columns=['id_x', 'id_y', 'id_genre'])
        .merge(df_movies, left_on='id_movie', right_on='id')
        .merge(df_aggregations, how='left', on='id_movie')
        .drop(columns=['id'])
        .rename(columns={'id_movie': 'id'})
    )

    df_exportation['qty_ratings'] = df_exportation['qty_ratings'].fillna(0).astype(int)

    logger.info('- TRANSFORM STEP EXECUTED WITH SUCCESS -')

    return df_exportation


def create_movie_table(engine: Engine):
    """
    Create the `movie` table if required
    """
    with engine.begin() as connection:
        connection.execute(
            text("""CREATE TABLE IF NOT EXISTS movie (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                genre VARCHAR(50) NOT NULL,
                release_date DATE NOT NULL,
                qty_ratings INT NOT NULL DEFAULT 0,
                avg_rating NUMERIC(3, 2),
                min_rating INT,
                max_rating INT,
                
                UNIQUE (name, release_date)
            );""")
        )

        connection.execute(text('TRUNCATE TABLE movie;'))

    logger.info('Table "movie" initialized with success')


def load_movie_table(engine: Engine, df: pd.DataFrame):
    try:
        df.to_sql(name='movie', con=engine, if_exists='append', index=False)
        logger.info('DataFrame successfully loaded into the movie table')

    except Exception as error:
        logger.error('Error loading data: %s', error)
        raise


def count_rows_movie_table(engine: Engine) -> None:
    with engine.connect() as connection:
        count_query = text('SELECT COUNT(*) FROM movie;')
        result = connection.execute(count_query)
        total_lines = result.scalar()

    logger.debug('The "movies" table currently contains %s line(s)', total_lines)


def load(df: pd.DataFrame):
    """
    Execute the Load step from the ETL process.
    """
    logger.info('- STARTING LOAD STEP -')

    conn = PostgresConnection(
        os.environ['POSTGRES_USER'],
        os.environ['POSTGRES_PASSWORD'],
        os.environ['POSTGRES_DB'],
        os.environ['POSTGRES_HOST'],
        os.environ['POSTGRES_PORT'],
    )

    try:
        engine = create_engine(conn.url)
        logger.debug('SQLAlchemy: Successful connection to Postgres')

    except Exception as error:
        logger.error('SQLAlchemy: Error configuring the engine: %s', error)
        raise

    try:
        create_movie_table(engine)
        count_rows_movie_table(engine)
        load_movie_table(engine, df)
        count_rows_movie_table(engine)

    finally:
        engine.dispose()

    logger.info('- LOAD STEP EXECUTED WITH SUCCCESS -')


def main():
    # Running the ETL process
    logger.info('-- STARTING ETL PROCESS --')
    load(transform(*extract()))
    logger.info('-- ETL PROCESS EXECUTED WITH SUCCESS --')

    logger.info('Checking the content of the `movie` table')
    conn = PostgresConnection(
        os.environ['POSTGRES_USER'],
        os.environ['POSTGRES_PASSWORD'],
        os.environ['POSTGRES_DB'],
        os.environ['POSTGRES_HOST'],
        os.environ['POSTGRES_PORT'],
    )

    engine = create_engine(conn.url)

    try:
        with engine.begin() as connection:
            query = text('SELECT * FROM movie;')
            df_movies = pd.read_sql_query(query, con=connection)
            logger.info('\n%s', df_movies.head())

    finally:
        engine.dispose()

    logger.info('Hopefully you liked my work.')


if __name__ == '__main__':
    main()
```

Logs returned by my code for evaluation:
```
2026-06-25 13:10:25,161 - ETL - INFO - -- STARTING ETL PROCESS --
2026-06-25 13:10:25,161 - ETL - INFO - - STARTING EXTRACT STEP -
2026-06-25 13:10:25,381 - ETL - DEBUG - Successul authentication to API.
2026-06-25 13:10:25,701 - ETL - INFO - Downloaded 3 genres from endpoint.
2026-06-25 13:10:26,303 - ETL - INFO - Downloaded 4 genre_movies from endpoint.
2026-06-25 13:10:27,047 - ETL - INFO - Downloaded 4 movies from endpoint.
2026-06-25 13:10:27,177 - ETL - INFO - Downloaded 6 ratings from endpoint.
2026-06-25 13:10:27,181 - ETL - INFO - - EXTRACT STEP EXECUTED WITH SUCCESS -
2026-06-25 13:10:27,181 - ETL - INFO - - STARTING TRANSFORM STEP -
2026-06-25 13:10:27,267 - ETL - INFO - - TRANSFORM STEP EXECUTED WITH SUCCESS -
2026-06-25 13:10:27,267 - ETL - INFO - - STARTING LOAD STEP -
2026-06-25 13:10:27,449 - ETL - DEBUG - SQLAlchemy: Successful connection to Postgres
2026-06-25 13:10:27,553 - ETL - INFO - Table "movie" initialized with success
2026-06-25 13:10:27,555 - ETL - DEBUG - The "movies" table currently contains 0 line(s)
2026-06-25 13:10:27,579 - ETL - INFO - DataFrame successfully loaded into the movie table
2026-06-25 13:10:27,580 - ETL - DEBUG - The "movies" table currently contains 4 line(s)
2026-06-25 13:10:27,581 - ETL - INFO - - LOAD STEP EXECUTED WITH SUCCCESS -
2026-06-25 13:10:27,581 - ETL - INFO - -- ETL PROCESS EXECUTED WITH SUCCESS --
2026-06-25 13:10:27,581 - ETL - INFO - Checking the content of the `movie` table
2026-06-25 13:10:27,606 - ETL - INFO - 
   id                name     genre release_date  qty_ratings  avg_rating  min_rating  max_rating
0   1  Mission Impossible    Action   1996-05-22            2         4.0         3.0         5.0
1   4            Die Hard    Action   1988-12-22            0         NaN         NaN         NaN
2   2         Scary Movie    Comedy   2000-07-07            2         4.5         4.0         5.0
3   3        Notting Hill  Romantic   1999-05-21            2         4.0         4.0         4.0
2026-06-25 13:10:27,617 - ETL - INFO - Hopefully you liked my work.
```
