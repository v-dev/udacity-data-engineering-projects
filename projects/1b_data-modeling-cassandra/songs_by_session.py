TABLE_NAME = 'songs_by_session'
drop = f'DROP TABLE IF EXISTS {TABLE_NAME}'

### create tables
## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

# select artist, song, length
# from music_app_history
# where sessionId = 338
# and itemInSession = 4
create = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    artist_name     TEXT,
    song            TEXT,
    length          DOUBLE,
    session_id      DECIMAL,
    item_in_session DECIMAL,
    PRIMARY KEY ( (session_id, item_in_session), artist_name )
);
"""

insert = f"""
INSERT INTO {TABLE_NAME} (artist_name, song, length, session_id, item_in_session)
VALUES (%s, %s, %s, %s, %s);
"""

select = f"""
SELECT artist_name, song, length, session_id, item_in_session
  FROM {TABLE_NAME}
 WHERE session_id = 338
   AND item_in_session = 4
"""

count = f"""
SELECT COUNT(1)
  FROM {TABLE_NAME}
"""