import pandas as pd
import pytest
from assertpy import assert_that
from sqlalchemy import create_engine


@pytest.fixture(scope="class")
def postgres_docker_conn():
    host_port = 'localhost:5432'
    postgres_docker_engine = create_engine(f"postgresql://{host_port}/sparkifydb?user=student&password=student",
                                           pool_recycle=3600)
    conn = postgres_docker_engine.connect()
    yield conn
    conn.close()


@pytest.fixture
def songplays_df(postgres_docker_conn):
    return pd.read_sql('select * from songplays', postgres_docker_conn)


@pytest.fixture
def songs_df(postgres_docker_conn):
    return pd.read_sql('select * from songs', postgres_docker_conn)


def test_songs_count(songs_df):
    count = songs_df.count()[0]
    expected_songs_rows = 71
    assert_that(count).is_equal_to(expected_songs_rows)


def test_songs_duration(songs_df):
    durations = songs_df['duration'].head().astype('double').values

    for duration in durations:
        has_5_precision(duration)


def test_users_count(postgres_docker_conn):
    users = pd.read_sql('select * from users', postgres_docker_conn)
    count = users.count()[0]
    expected_users_rows = 96
    assert_that(count).is_equal_to(expected_users_rows)


def test_artists_count(postgres_docker_conn):
    artists = pd.read_sql('select * from artists', postgres_docker_conn)
    count = artists.count()[0]
    expected_artists_rows = 69
    assert_that(count).is_equal_to(expected_artists_rows)


def has_5_precision(a_double):
    """
    I realize this isn't perfect, but will suffice for just head(5) dataset
    """
    to_five_places = f'{a_double:.5f}'
    to_string = str(a_double)
    print(f"original: {a_double}, to 5 places: {to_five_places}, to string: {to_string}")
    assert_that(to_five_places).is_equal_to(to_string)


def test_songsplays_with_artist(postgres_docker_conn):
    df = pd.read_sql('SELECT * FROM songplays WHERE song_id is NOT NULL and artist_id is NOT NULL',
                     postgres_docker_conn)
    count = df.count()[0]
    assert_that(count).is_equal_to(1)


def test_songplays_count(songplays_df):
    count = songplays_df.count()[0]
    expected_songplays_rows = 6820
    assert_that(count).is_equal_to(expected_songplays_rows)
