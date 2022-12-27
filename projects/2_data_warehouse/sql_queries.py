import config

# TABLE NAMES
STG_EVENTS_TABLE = 'stg_events'
STG_SONGS_TABLE = 'stg_songs'

SONGPLAYS_TABLE = 'songplays'
USERS_TABLE = 'users'
SONGS_TABLE = "songs"
ARTISTS_TABLE = 'artists'
TIME_TABLE = 'time'

# DROP TABLES
staging_events_table_drop = f"DROP TABLE IF EXISTS {STG_EVENTS_TABLE}"
staging_songs_table_drop = f"DROP TABLE IF EXISTS {STG_SONGS_TABLE}"
songplay_table_drop = f"DROP TABLE IF EXISTS {SONGPLAYS_TABLE}"
user_table_drop = f"DROP TABLE IF EXISTS {USERS_TABLE}"
song_table_drop = f"DROP TABLE IF EXISTS {SONGS_TABLE}"
artist_table_drop = f"DROP TABLE IF EXISTS {ARTISTS_TABLE}"
time_table_drop = f"DROP TABLE IF EXISTS {TIME_TABLE}"

# CREATE TABLES
staging_events_table_create = (f"""
CREATE TABLE IF NOT EXISTS {STG_EVENTS_TABLE} (
    artist          VARCHAR(500),
    auth            VARCHAR(500),
    first_name      VARCHAR(500),
    gender          VARCHAR(100),
    item_in_session INT,
    last_name       VARCHAR(500),
    length          NUMERIC,
    level           VARCHAR(100),
    location        VARCHAR(500),
    method          VARCHAR(100),
    page            VARCHAR(100),
    registration    VARCHAR(100),
    session_id      INT,
    song            VARCHAR(500),
    status          INT,
    ts              VARCHAR(100),
    user_agent      VARCHAR(1000),
    user_id         INT
)
""")

staging_songs_table_create = (f"""
CREATE TABLE IF NOT EXISTS {STG_SONGS_TABLE} (
    num_songs        INT,
    artist_id        VARCHAR(100),
    artist_latitude  NUMERIC(7, 2),
    artist_longitude NUMERIC(7, 2),
    artist_location  VARCHAR(500),
    artist_name      VARCHAR(1000),
    song_id          VARCHAR(100),
    title            VARCHAR(500),
    duration         NUMERIC(8, 3),
    year             INT
)
""")

songplay_table_create = (f"""
CREATE TABLE IF NOT EXISTS {SONGPLAYS_TABLE} (
    songplay_id BIGINT IDENTITY (0,1) DISTKEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     INT       NOT NULL,
    level       VARCHAR(100),
    song_id     VARCHAR(100),
    artist_id   VARCHAR(100),
    session_id  INT,
    location    VARCHAR(500),
    user_agent  TEXT
)
""")

user_table_create = (f"""
CREATE TABLE IF NOT EXISTS {USERS_TABLE} (
    user_id    INT PRIMARY KEY DISTKEY,
    first_name VARCHAR(500),
    last_name  VARCHAR(500),
    gender     VARCHAR(100),
    level      VARCHAR(100)
)
""")

song_table_create = (f"""
CREATE TABLE IF NOT EXISTS {SONGS_TABLE} (
    song_id   VARCHAR(100) PRIMARY KEY DISTKEY,
    title     TEXT    NOT NULL,
    artist_id VARCHAR(100),
    year      INT,
    duration  NUMERIC(9,5) NOT NULL
)
""")

artist_table_create = (f"""
CREATE TABLE IF NOT EXISTS {ARTISTS_TABLE} (
    artist_id VARCHAR(100) PRIMARY KEY DISTKEY,
    name      VARCHAR(500) NOT NULL,
    location  VARCHAR(500),
    latitude  DOUBLE PRECISION,
    longitude DOUBLE PRECISION
)
""")

time_table_create = (f"""
CREATE TABLE IF NOT EXISTS {TIME_TABLE} (
    start_time TIMESTAMP PRIMARY KEY DISTKEY,
    hour       INT,
    day        INT,
    week       INT,
    month      VARCHAR(20),
    year       VARCHAR(20),
    weekday    BOOLEAN
)
""")

# STAGING TABLES
staging_events_copy = (f"""
COPY {STG_EVENTS_TABLE} FROM '{config.LOG_DATA}'
CREDENTIALS 'aws_iam_role={config.ARN}'
JSON '{config.LOG_JSONPATH}'
REGION 'us-west-2';
""")

staging_songs_copy = (f"""
COPY {STG_SONGS_TABLE} FROM '{config.SONG_DATA}'
CREDENTIALS 'aws_iam_role={config.ARN}'
JSON 'auto'
REGION 'us-west-2';
""")

# FINAL TABLES
songplay_table_insert = (f"""
INSERT INTO {SONGPLAYS_TABLE} (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (SELECT TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    e.location,
    e.user_agent
    FROM {STG_EVENTS_TABLE} e
    JOIN {STG_SONGS_TABLE} s ON (e.song = s.title AND e.artist = s.artist_name)
    WHERE e.page = 'NextSong')
""")

user_table_insert = (f"""
INSERT INTO {USERS_TABLE}
WITH ordered_staging_events AS
         (SELECT user_id,
                 first_name,
                 last_name,
                 gender,
                 level,
                 ROW_NUMBER() over (PARTITION BY user_id ORDER BY ts DESC) as rank
          FROM {STG_EVENTS_TABLE}
          WHERE user_id IS NOT NULL
            AND page = 'NextSong')
SELECT user_id, first_name, last_name, gender, level
FROM ordered_staging_events e
WHERE rank = 1
""")

song_table_insert = (f"""
INSERT INTO {SONGS_TABLE}
(SELECT song_id, title, artist_id, year, duration FROM {STG_SONGS_TABLE})
""")

artist_table_insert = (f"""
INSERT INTO {ARTISTS_TABLE}
WITH ranked_songs AS (SELECT artist_id,
                             artist_name,
                             artist_location,
                             artist_latitude,
                             artist_longitude,
                             row_number() OVER (PARTITION BY artist_id ORDER BY artist_id) AS rank
                      FROM {STG_SONGS_TABLE}
                      ORDER BY rank)
SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM ranked_songs
WHERE rank = 1
""")

time_table_insert = (f"""
INSERT INTO {TIME_TABLE}
WITH times AS (select TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS newts,
                      EXTRACT(hour FROM newts)                              AS hour,
                      EXTRACT(day FROM newts)                               AS day,
                      EXTRACT(week FROM newts)                              AS week,
                      EXTRACT(month FROM newts)                             AS month,
                      EXTRACT(year FROM newts)                              AS year,
                      CASE
                          WHEN EXTRACT(dow FROM newts) IN (1, 2, 3, 4, 5) THEN true
                          ELSE false END                                    AS weekday
               FROM {STG_EVENTS_TABLE})
SELECT distinct newts::timestamp, hour, day, week, month, year, weekday
FROM times
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
