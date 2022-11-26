"""
SQL for query 2:
Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for
userid = 10, sessionid = 182

Which, in SQL is:
SELECT artist_name, song, user
  FROM songs_by_player
 WHERE user_id = 10
   AND session_id = 182
;
"""

TABLE_NAME = 'songs_by_player'
drop = f'DROP TABLE IF EXISTS {TABLE_NAME}'

create = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    user_id         DECIMAL,
    session_id      DECIMAL,
    item_in_session DECIMAL,
    artist_name     TEXT,
    song            TEXT,
    user            TEXT,
    PRIMARY KEY ( (user_id, session_id), item_in_session )
);
"""

insert = f"""
INSERT INTO {TABLE_NAME} (user_id, session_id, item_in_session, artist_name, song, user)
VALUES (%s, %s, %s, %s, %s, %s);
"""

select = f"""
SELECT artist_name, song, user
  FROM {TABLE_NAME}
 WHERE user_id = 10
   AND session_id = 182
"""

count = f"""
SELECT COUNT(1)
  FROM {TABLE_NAME}
"""
