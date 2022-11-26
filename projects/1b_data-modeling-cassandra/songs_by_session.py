"""
SQL for query 1:
Give me the artist, song title and song's length in the music app history that was heard during
sessionId = 338, and itemInSession = 4

Which, in SQL is:
SELECT artist_name, song, length
  FROM songs_by_session
 WHERE session_id = 338
   AND item_in_session = 4
;
"""

TABLE_NAME = 'songs_by_session'
drop = f'DROP TABLE IF EXISTS {TABLE_NAME}'

create = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    session_id      DECIMAL,
    item_in_session DECIMAL,
    artist_name     TEXT,
    song            TEXT,
    length          DOUBLE,
    PRIMARY KEY ( session_id, item_in_session )
);
"""

insert = f"""
INSERT INTO {TABLE_NAME} (session_id, item_in_session, artist_name, song, length)
VALUES (%s, %s, %s, %s, %s);
"""

select = f"""
SELECT artist_name, song, length
  FROM {TABLE_NAME}
 WHERE session_id = 338
   AND item_in_session = 4
"""

count = f"""
SELECT COUNT(1)
  FROM {TABLE_NAME}
"""
