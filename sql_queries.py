import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get("S3", "LOG_DATA")
IAM_ROLE = config.get("IAM_ROLE", "ARN")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES
# saving config links in their own variables
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE Schema for the ttaging tables to save data from S3 buckets to later shift to the STAR schema files
# These tables will be created from created_tables.py file
staging_events_table_create= ("""
CREATE TABLE staging_events (
        artist            TEXT,                 
        auth              VARCHAR,
        firstName         VARCHAR,
        gender            VARCHAR(1),
        ItemInSession     INT,
        lastName          VARCHAR,
        length            FLOAT,
        level             VARCHAR,
        location          VARCHAR,
        method            VARCHAR,
        page              VARCHAR,
        registration      VARCHAR,
        sessionId         INTEGER,
        song              VARCHAR,
        status            INTEGER,
        ts                BIGINT, 
        userAgent         VARCHAR, 
        userId            INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
        song_id            VARCHAR,
        artist_id          VARCHAR,
        artist_latitude    FLOAT,
        artist_longitude   FLOAT,
        artist_location    TEXT,                    
        artist_name        TEXT,                    
        duration           FLOAT,
        num_songs          INT,
        title              VARCHAR,
        year               INT );
""")

# Create the STAR Schema to insert data later
# First create the fact talbe songplay
songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id   INT IDENTITY(0,1) PRIMARY KEY, 
    start_time    TIMESTAMP NOT NULL, 
    user_id       INT NOT NULL, 
    level         VARCHAR, 
    song_id       VARCHAR, 
    artist_id     VARCHAR, 
    session_id    INT, 
    location      VARCHAR, 
    user_agent    VARCHAR);
""")

# Then create the dimension tables - users, songs, artists, time
user_table_create = ("""
CREATE TABLE users (
    user_id      INT PRIMARY KEY NOT NULL, 
    first_name   TEXT, 
    last_name    TEXT, 
    gender       VARCHAR(1), 
    level        VARCHAR);
""")

song_table_create = ("""
CREATE TABLE songs(
    song_id       VARCHAR PRIMARY KEY, 
    title         VARCHAR NOT NULL, 
    artist_id     VARCHAR NOT NULL, 
    year          INT, 
    duration      FLOAT);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id     VARCHAR PRIMARY KEY, 
    name          TEXT, 
    location      TEXT, 
    latitude      FLOAT, 
    longitude     FLOAT);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time    TIMESTAMP PRIMARY KEY, 
    hour          INT, 
    day           INT, 
    week          INT, 
    month         INT, 
    year          INT, 
    weekday       VARCHAR);
""")

# STAGING TABLES
# The follwoing tables are created to stage the tables from S3 files. 
#The log data uses JSON formatting. Naturally, it uses IAM roles and S3 links. 
#These staging tables will be run from etl.py file.

staging_events_copy = ("""
COPY staging_events
FROM {0}
IAM_ROLE {1}
FORMAT AS JSON {2}
REGION 'us-west-2';
""").format(
    LOG_DATA,IAM_ROLE, LOG_JSONPATH,
)


staging_songs_copy = ("""
COPY staging_songs
FROM {0}
IAM_ROLE {1}
FORMAT AS JSON 'auto'
REGION 'us-west-2';;
""").format(
    SONG_DATA, IAM_ROLE
)


# FINAL TABLES
# After the schemas are created and the data is parked in the staging tables, 
# following queries will insert/load the data from the staging tables to the fact and dimension tables
songplay_table_insert = ("""
INSERT INTO songplays
(
        start_time
        ,user_id
        ,level
        ,song_id
        ,artist_id
        ,session_id
        ,location
        ,user_agent
)      
SELECT DISTINCT 
        event.ts / 1000 * interval '1 second' + TIMESTAMP 'epoch' AS start_time,
        event.userid,
        event.level,
        song.song_id,
        song.artist_id,
        event.sessionid,
        event.location,
        event.useragent
FROM    staging_events  event
JOIN    staging_songs   song  
ON      song.artist_name  =   event.artist
AND     event.page    =   'NextSong'
AND     event.song     =    song.title 
""")


user_table_insert = ("""
INSERT INTO users
(
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level 
)
SELECT DISTINCT
    event.userId, 
    event.firstName, 
    event.lastName, 
    event.gender,
    event.level
FROM    staging_events  event
WHERE event.userId IS NOT NULL
    AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
(
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
)
SELECT DISTINCT
    song.song_id, song.title, song.artist_id,
    song.year, song.duration
FROM staging_songs song    
WHERE song_id IS NOT NULL;                
""")

artist_table_insert = ("""
INSERT INTO artists
(
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
SELECT DISTINCT
    song.artist_id, 
    song.artist_name,
    song.artist_location,
    song.artist_latitude,
    song.artist_longitude
FROM staging_songs song
WHERE artist_id IS NOT NULL;                     
""")

"""
CREATE TABLE staging_events (
        artist            TEXT,                 
        auth              VARCHAR,
        firstName         VARCHAR,
        gender            VARCHAR(1),
        ItemInSession     INT,
        lastName          VARCHAR,
        length            FLOAT,
        level             VARCHAR,
        location          VARCHAR,
        method            VARCHAR,
        page              VARCHAR,
        registration      VARCHAR,
        sessionId         INTEGER,
        song              VARCHAR,
        status            INTEGER,
        ts                BIGINT, 
        userAgent         VARCHAR, 
        userId            INTEGER);

CREATE TABLE staging_songs (
        song_id            VARCHAR,
        artist_id          VARCHAR,
        artist_latitude    FLOAT,
        artist_longitude   FLOAT,
        artist_location    TEXT,                    
        artist_name        TEXT,                    
        duration           FLOAT,
        num_songs          INT,
        title              VARCHAR,
        year               INT );
"""

time_table_insert = ("""
INSERT INTO time
(
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)
SELECT DISTINCT
    ts_timestamp AS start_time,
    EXTRACT(hour FROM ts_timestamp),
    EXTRACT(day FROM ts_timestamp),
    EXTRACT(week FROM ts_timestamp),
    EXTRACT(month FROM ts_timestamp),
    EXTRACT(year FROM ts_timestamp),
    EXTRACT(weekday FROM ts_timestamp)
FROM (
    SELECT TIMESTAMP 'epoch' + event.ts / 1000 * INTERVAL '1 second' AS ts_timestamp
    FROM staging_events event
    WHERE page = 'NextSong'
);                 
""")

# QUERY LISTS - the create_table_queries and drop_table_queries lists will be run from create_tables.py
# copy_table_queries and insert_table_queries will be run from etl.py

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
