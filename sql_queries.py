import configparser

# Load external Configuration
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE','ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSON_PATH = config.get('S3', 'LOG_JSON_PATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession INT,
lastName VARCHAR,
length FLOAT,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId NUMERIC,
song VARCHAR,
status SMALLINT,
ts TIMESTAMP,
userAgent VARCHAR,
userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
artist_id VARCHAR,
artist_latitude FLOAT,
artist_location VARCHAR,
artist_longitude FLOAT,
artist_name VARCHAR,
duration FLOAT,
num_songs INT,
song_id VARCHAR,
title VARCHAR,
year SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE song_plays (
songplay_id BIGINT IDENTITY(1,1),
start_time TIMESTAMP NOT NULL,
user_id VARCHAR NOT NULL SORTKEY,
level VARCHAR(10) NOT NULL,
song_id VARCHAR NOT NULL,
artist_id VARCHAR NOT NULL DISTKEY, 
sessionid INT NOT NULL,
location VARCHAR,
user_agent VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE users (
user_id VARCHAR NOT NULL SORTKEY, 
first_name VARCHAR NOT NULL, 
last_name VARCHAR NOT NULL, 
gender VARCHAR(1) NOT NULL, 
level VARCHAR(10) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE songs (
song_id VARCHAR NOT NULL, 
title VARCHAR NOT NULL, 
artist_id VARCHAR NOT NULL DISTKEY, 
year SMALLINT SORTKEY,
duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id VARCHAR NOT NULL DISTKEY, 
name VARCHAR NOT NULL, 
location VARCHAR, 
latitude FLOAT,
longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE time (
start_time TIMESTAMP NOT NULL SORTKEY,
hour SMALLINT NOT NULL,
day SMALLINT NOT NULL,
week SMALLINT NOT NULL,
month SMALLINT NOT NULL,
year SMALLINT NOT NULL,
weekday SMALLINT NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT AS JSON {} 
    TIMEFORMAT as 'epochmillisecs'
    COMPUPDATE OFF region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSON_PATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS JSON 'auto' 
    COMPUPDATE OFF region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO song_plays (start_time, user_id, level, song_id, artist_id, sessionid, location, user_agent)
SELECT DISTINCT (sevents.ts) AS start_time, sevents.userId AS user_id, sevents.level AS level, 
ssongs.artist_id AS song_id, ssongs.artist_id AS artist_id, sevents.sessionid AS sessionid, sevents.location AS location, 
sevents.userAgent AS user_agent
FROM staging_events sevents JOIN staging_songs ssongs 
 ON (sevents.artist = ssongs.artist_name AND sevents.song = ssongs.title)
WHERE sevents.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT (userid) AS user_id, firstname AS first_name, lastname AS last_name, gender AS gender, level AS level
FROM staging_events 
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT (song_id) AS song_id, title AS title, artist_id AS artist_id, year AS year, duration AS duration
FROM staging_songs 
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT (artist_id) AS artist_id, title AS title, artist_location AS location, 
artist_latitude AS latitude, artist_longitude AS longitude
FROM staging_songs 
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT (ts) AS start_time, 
EXTRACT(hour FROM start_time) AS hour,
EXTRACT(day FROM start_time) AS day,
EXTRACT(week FROM start_time) AS week,
EXTRACT(month FROM start_time) AS month,
EXTRACT(year FROM start_time) AS year,
DATE_PART(dow, start_time) AS weekday
FROM staging_events 
WHERE page = 'NextSong';
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
