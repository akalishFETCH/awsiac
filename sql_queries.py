import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')
SONGS_JSONPATH  = config.get('S3', 'SONGS_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE staging_events_table (
    event_id          BIGINT IDENTITY(0,1) NOT NULL primary key
    ,artist           VARCHAR
    ,auth             VARCHAR
    ,firstName        VARCHAR
    ,gender           VARCHAR
    ,itemInSession    VARCHAR
    ,lastName         VARCHAR
    ,length           VARCHAR
    ,level            VARCHAR
    ,location         VARCHAR
    ,method           VARCHAR
    ,page             VARCHAR
    ,registration     VARCHAR
    ,sessionId        integer NOT NULL SORTKEY DISTKEY
    ,song             VARCHAR
    ,status           VARCHAR
    ,ts               BIGINT
    ,userAgent        VARCHAR
    ,userId           integer);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs_table (
                num_songs           INTEGER              NULL,
                artist_id           varchar(MAX)         NOT NULL SORTKEY DISTKEY,
                artist_latitude     varchar(MAX)         NULL,
                artist_longitude    varchar(MAX)         NULL,
                artist_location     varchar(MAX)         NULL,
                artist_name         varchar(MAX)         NULL,
                song_id             varchar(MAX)         NOT NULL,
                title               varchar(MAX)         NULL,
                duration            DECIMAL(9)           NULL,
                year                INTEGER              NULL);
""")

songplay_table_create = ("""CREATE TABLE songplay_table (
    songplay_id    integer identity(0,1) primary key sortkey
    ,start_time    timestamp             not null
    ,user_id       varchar(MAX)          not null distkey
    ,level         VARCHAR               not null
    ,song_id       varchar(MAX)          not null
    ,artist_id     varchar(MAX)          not null
    ,session_id    integer               not null
    ,location      VARCHAR
    ,user_agent    VARCHAR);
""")

user_table_create = ("""CREATE TABLE user_table (
    user_id       varchar(MAX) not null primary key sortkey
    ,first_name   VARCHAR
    ,last_name    VARCHAR
    ,gender       VARCHAR
    ,level        VARCHAR);
""")

song_table_create = ("""CREATE TABLE song_table (
    song_id       varchar(MAX) not null primary key sortkey
    ,title        VARCHAR
    ,artist_id    VARCHAR
    ,year         smallint
    ,duration     numeric);
""")

artist_table_create = ("""CREATE TABLE artist_table (
    artist_id      varchar(MAX) not null primary key sortkey
    ,name         VARCHAR(MAX)
    ,location     VARCHAR(MAX)
    ,latitude     numeric
    ,longitude    numeric);
""")

time_table_create = ("""CREATE TABLE time_table (
    start_time    timestamp  not null sortkey
    ,hour         smallint
    ,day          smallint
    ,week         smallint
    ,month        smallint
    ,year         smallint
    ,weekday      smallint);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs_table FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)


# FINAL TABLES


songplay_table_insert = ("""
    INSERT INTO songplay_table (        start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent)
                                        
    SELECT  DISTINCT TIMESTAMP 'epoch' + event.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            event.userId                   AS user_id,
            event.level                    AS level,
            song.song_id                  AS song_id,
            song.artist_id                AS artist_id,
            event.sessionId                AS session_id,
            event.location                 AS location,
            event.userAgent                AS user_agent
    FROM staging_events_table AS event
    JOIN staging_songs_table AS song
        ON (event.artist = song.artist_name)
    WHERE event.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO user_table (                 user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
    SELECT  DISTINCT event.userId          AS user_id,
            event.firstName                AS first_name,
            event.lastName                 AS last_name,
            event.gender                   AS gender,
            event.level                    AS level
    FROM staging_events_table AS event
    WHERE event.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO song_table (                 song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
    SELECT  DISTINCT song.song_id         AS song_id,
            song.title                    AS title,
            song.artist_id                AS artist_id,
            song.year                     AS year,
            song.duration                 AS duration
    FROM staging_songs_table AS song;
""")

artist_table_insert = ("""
    INSERT INTO artist_table (               artist_id,
                                        name,
                                        location,
                                        latitude,
                                        longitude)
    SELECT  DISTINCT song.artist_id       AS artist_id,
            song.artist_name              AS name,
            song.artist_location          AS location,
            song.artist_latitude          AS latitude,
            song.artist_longitude         AS longitude
    FROM staging_songs_table AS song;
""")


time_table_insert = ("""
    INSERT INTO time_table (                  start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + event.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events_table AS event
    WHERE event.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
