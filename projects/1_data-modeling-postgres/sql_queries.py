# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     INT       NOT NULL,
    level       VARCHAR(100),
    song_id     VARCHAR(100),
    artist_id   VARCHAR(100),
    session_id  INT,
    location    VARCHAR(500),
    user_agent  TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    INT PRIMARY KEY,
    first_name VARCHAR(500),
    last_name  VARCHAR(500),
    gender     VARCHAR(100),
    level      VARCHAR(100)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   VARCHAR(100) PRIMARY KEY,
    title     TEXT    NOT NULL,
    artist_id VARCHAR(100),
    year      INT,
    duration  NUMERIC(8,5) NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(100) PRIMARY KEY,
    name      VARCHAR(500) NOT NULL,
    location  VARCHAR(500),
    latitude  DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP,
    hour       INT,
    day        INT,
    week       INT,
    month      VARCHAR(20),
    year       VARCHAR(20),
    weekday    VARCHAR(20)
);
""")

# INSERT RECORDS

# 9 columns for songplays table
songplay_table_insert = ("""
INSERT INTO songplays
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) 
DO NOTHING
""")

# 5 columns for users table
user_table_insert = ("""
INSERT INTO users
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT(user_id)
DO UPDATE SET level=EXCLUDED.level; 
""")

song_table_insert = ("""
INSERT INTO songs
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING
""")

time_table_insert = ("""
INSERT INTO time
VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT song_id, a.artist_id
  FROM songs s
       JOIN artists a
       ON s.artist_id = a.artist_id
 WHERE s.title = %s
   AND a.name = %s
   AND s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
