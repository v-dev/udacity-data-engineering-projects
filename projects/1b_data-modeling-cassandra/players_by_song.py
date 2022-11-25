TABLE_NAME = 'players_by_song'
drop = f'DROP TABLE IF EXISTS {TABLE_NAME}'

## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
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

# Query 3: Give me every user name (first and last) in my music app history who listened to the
# song 'All Hands Against His Own'
select = f"""
SELECT user
  FROM {TABLE_NAME}
 WHERE song = 'All Hands Against His Own'
"""

count = f"""
SELECT COUNT(1)
  FROM {TABLE_NAME}
"""
