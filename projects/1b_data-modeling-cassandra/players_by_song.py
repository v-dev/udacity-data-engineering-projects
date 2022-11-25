"""
SQL for query 3:
Query 3: Give me every user name (first and last) in my music app history who listened to
the song 'All Hands Against His Own'

Which, in SQL is:
SELECT user
  FROM players_by_song
 WHERE song = 'All Hands Against His Own';
"""

TABLE_NAME = 'players_by_song'
drop = f'DROP TABLE IF EXISTS {TABLE_NAME}'

create = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    song    TEXT,
    user_id DECIMAL,
    user    TEXT,
    PRIMARY KEY ( song, user_id )
);
"""

insert = f"""
INSERT INTO {TABLE_NAME} (song, user_id, user)
VALUES (%s, %s, %s);
"""

select = f"""
SELECT user
  FROM {TABLE_NAME}
 WHERE song = 'All Hands Against His Own'
"""

count = f"""
SELECT COUNT(1)
  FROM {TABLE_NAME}
"""
