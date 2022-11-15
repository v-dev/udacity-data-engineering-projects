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


def test_songplays_count(songplays_df):
    count = songplays_df.count()[0]
    expected_songplays_rows = 6820
    assert_that(count).is_equal_to(expected_songplays_rows)


def test_songs_duration(songs_df):
    durations = songs_df['duration'].head()
    print(f"durations: {durations}")
