import pandas as pd
import pytest
from cassandra.cluster import Cluster

import players_by_song
import songs_by_player
import songs_by_session

TOTAL_ROWS = 6820


@pytest.fixture(scope="class")
def session():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('udacity')
    yield session
    session.shutdown()
    cluster.shutdown()


def test_songs_by_session(session):
    """
    Validate the "songs_by_session" table has been populated properly

    Use known query to select from table and validate expected row's existence
    Verify the correct result count
    """
    count = pd.DataFrame(list(session.execute(songs_by_session.count)))
    assert count.values[0] == TOTAL_ROWS

    songs_by_session_df = pd.DataFrame(list(session.execute(songs_by_session.select)))
    assert songs_by_session_df.count()[0] == 1
    song = songs_by_session_df['song'].values[0]
    assert song == "Music Matters (Mark Knight Dub)"


def test_songs_by_player(session):
    """
    Validate the "songs_by_player" table has been populated properly

    Use known query to select from table and validate expected rows' existence
    Verify the correct result count
    """
    count = pd.DataFrame(list(session.execute(songs_by_player.count)))
    assert count.values[0] == TOTAL_ROWS

    songs_by_player_df = pd.DataFrame(list(session.execute(songs_by_player.select)))
    assert songs_by_player_df.count()[0] == 4
    songs_played_results = songs_by_player_df['song'].values
    assert "Keep On Keepin' On" in songs_played_results
    assert "Greece 2000" in songs_played_results
    assert "Kilometer" in songs_played_results
    assert "Catch You Baby (Steve Pitron & Max Sanna Radio Edit)" in songs_played_results


def test_players_by_song(session):
    """
    Validate the "players_by_song" table has been populated properly

    Use known query to select from table and validate expected rows' existence
    Verify the correct result count
    """
    count = pd.DataFrame(list(session.execute(players_by_song.count)))
    assert count.values[0] == 6618

    players_by_song_df = pd.DataFrame(list(session.execute(players_by_song.select)))
    assert players_by_song_df.count()[0] == 3
    players_by_song_results = players_by_song_df['user'].values
    assert "Tegan Levine" in players_by_song_results
    assert "Sara Johnson" in players_by_song_results
    assert "Jacqueline Lynch" in players_by_song_results
