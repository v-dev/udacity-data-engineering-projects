# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays (
songplay_id int primary key,
start_time timestamp NOT NULL,
user_id int NOT NULL,
level varchar(100),
song_id varchar(100),
artist_id varchar(100),
session_id int,
location varchar(500),
user_agent text
);
""")

user_table_create = ("""
create table if not exists users (
user_id int primary key,
first_name varchar(500),
last_name varchar(500),
gender varchar(100),
level varchar(100)
)
""")

song_table_create = ("""
create table if not exists songs (
song_id varchar(100) primary key,
title text NOT NULL ,
artist_id varchar(100),
year int,
duration numeric NOT NULL
);
""")

artist_table_create = ("""
create table if not exists artists (
artist_id varchar(100) primary key,
name varchar(500) NOT NULL,
location varchar(500),
latitude double precision,
longitude double precision
);
""")

time_table_create = ("""
create table if not exists time (
start_time timestamp,  -- maybe this should be a bigint or varchar? it's epoch format
hour int,
day int,
week int,
month varchar(20),
year varchar(20),
weekday varchar(20)
);
""")

# INSERT RECORDS

# 9 columns for songplays table
songplay_table_insert = ("""
INSERT INTO songplays VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) 
DO NOTHING
""")

# 5 columns for users table
user_table_insert = ("""
INSERT INTO users VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO NOTHING
""")

song_table_insert = ("""
insert into songs values (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
DO NOTHING
""")

artist_table_insert = ("""
insert into artists values (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING
""")


time_table_insert = ("""
insert into time values (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
select song_id, a.artist_id
from songs s
join artists a on s.artist_id = a.artist_id
where s.title = %s
and a.name = %s
and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]