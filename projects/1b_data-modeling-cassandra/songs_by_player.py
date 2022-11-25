TABLE_NAME = 'songs_by_player'
drop = f'DROP TABLE IF EXISTS {TABLE_NAME}'

## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

# select artist, song
# from songs_played
# where user_id = 10
# and session_id = 182
create = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    artist_name     TEXT,
    song            TEXT,
    user            TEXT,
    user_id         DECIMAL,
    session_id      DECIMAL,
    item_in_session DECIMAL,
    PRIMARY KEY ( (user_id, session_id), item_in_session )
);
"""

insert = f"""
INSERT INTO {TABLE_NAME} (artist_name, song, user, user_id, session_id, item_in_session)
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