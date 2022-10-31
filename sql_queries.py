import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config.get('S3', 'LOG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
CREDENTIALS = 'aws_iam_role=' + ARN.replace("'", "")


# DROP TABLES
table_names = [
    'staging_events',
    'staging_songs',
    'songplay',
    'sparkify_user',
    'song_info',
    'artist_details',
    'start_time']


def drop_table(table_names):
    queries = []
    for table_name in table_names:
        query = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        queries.append(query)
    return queries


# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE staging_events
    (
        artist          VARCHAR(300),
        auth            VARCHAR(25),
        first_name      VARCHAR(25)   distkey,
        gender          VARCHAR(1),
        item_in_session INT,
        last_name       VARCHAR(25)   ,
        legnth          DECIMAL(9, 5) sortkey,
        level           VARCHAR(10),
        location        VARCHAR(300),
        method          VARCHAR(6),
        page            VARCHAR(50),
        registration    DECIMAL(14, 1),
        session_id      INT,
        song            VARCHAR(300)  ,
        status          INT,
        ts              BIGINT,
        user_agent      VARCHAR(150),
        user_id         VARCHAR(10)
    )
                            """)


staging_songs_table_create = ("""
    CREATE TABLE staging_songs
    (
        num_songs        INT,
        artist_id        VARCHAR(25),
        artist_latitude  DECIMAL(10, 5),
        artist_longitude DECIMAL(10, 5),
        artist_location  VARCHAR(300),
        artist_name      VARCHAR(300) ,
        song_id          VARCHAR(25)  ,
        title            VARCHAR(300)  ,
        duration         DECIMAL(9, 5) ,
        year             INT
    )
                            """)

songplay_table_create = ("""
    CREATE TABLE songplay
    (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time  TIMESTAMP NOT NULL,
        user_id     VARCHAR(10),
        level       VARCHAR(10),
        song_id     VARCHAR(300) NOT NULL,
        artist_id   VARCHAR(25) NOT NULL,
        session_id  INT,
        location    VARCHAR(300),
        user_agent  VARCHAR(150)
    )
    diststyle all;
                        """)

user_table_create = ("""
    CREATE TABLE sparkify_user
    (
        user_id    VARCHAR(10) PRIMARY KEY,
        first_name VARCHAR(25),
        last_name  VARCHAR(25),
        gender     VARCHAR(1),
        level      VARCHAR(10)
    )
    diststyle all;
                    """)

song_table_create = ("""
    CREATE TABLE song_info
    (
        song_id   VARCHAR(25) PRIMARY KEY,
        title     VARCHAR(300) NOT NULL,
        artist_id VARCHAR(25),
        year      INT,
        duration  DECIMAL(9, 5) NOT NULL
    )
    diststyle all;
                    """)

artist_table_create = ("""
    CREATE TABLE artist_details
    (
        artist_id VARCHAR(25) PRIMARY KEY,
        name      VARCHAR(300) NOT NULL,
        location  VARCHAR(300),
        lattitude DECIMAL(10, 5),
        longitude DECIMAL(10, 5)
    )
    diststyle all;
                    """)

time_table_create = ("""
    CREATE TABLE start_time
    (
        start_time TIMESTAMP PRIMARY KEY sortkey,
        hour       INT,
        day        INT,
        week       INT,
        month      INT,
        year       INT,
        weekday    INT
    )
    diststyle all;
                    """)

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events FROM {LOG_DATA}
    CREDENTIALS '{CREDENTIALS}'
    region \'us-west-2\'
    FORMAT AS JSON {LOG_JSONPATH};
""")


staging_songs_copy = (f"""
    COPY staging_songs FROM {SONG_DATA}
    CREDENTIALS '{CREDENTIALS}'
    region 'us-west-2'
    FORMAT AS JSON 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
     INSERT INTO songplay
                (
                            start_time,
                            user_id,
                            level,
                            song_id,
                            artist_id,
                            session_id,
                            location,
                            user_agent
                )
    SELECT     timestamp 'epoch' + (se.ts / 1000) * interval '1 second',
               se.user_id,
               se.level,
               ss.song_id,
               ss.artist_id,
               se.session_id,
               se.location,
               se.user_agent
    FROM       staging_events se
    INNER JOIN staging_songs ss
    ON         se.song = ss.title
    AND        se.artist = ss.artist_name
    AND        se.legnth = ss.duration
    WHERE      se.page = 'NextSong'
                        """)

user_table_insert = ("""
     INSERT INTO sparkify_user
                (user_id,
                 first_name,
                 last_name,
                 gender,
                 level)
    SELECT DISTINCT se.user_id,
           se.first_name,
           se.last_name,
           se.gender,
           se.level
    FROM   staging_events se
    WHERE  NOT EXISTS (SELECT 1
                       FROM   staging_events se2
                       WHERE  se.user_id = se2.user_id
                              AND se.ts < se2.ts)
                    """)

song_table_insert = ("""
    INSERT INTO song_info
                (song_id,
                 title,
                 artist_id,
                 year,
                 duration)
    SELECT DISTINCT ss.song_id,
           ss.title,
           ss.artist_id,
           CASE
             WHEN ss.year != 0 THEN ss.year
             ELSE NULL
           END AS year,
           ss.duration
    FROM   staging_songs ss
""")

artist_table_insert = ("""
    INSERT INTO artist_details
                (artist_id,
                 NAME,
                 location,
                 lattitude,
                 longitude)
    SELECT DISTINCT artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
    FROM   (SELECT ss.artist_id,
                   ss.artist_name,
                   ss.artist_location,
                   ss.artist_latitude,
                   ss.artist_longitude,
                   Row_number()
                     OVER(
                       partition BY ss.artist_id
                       ORDER BY ss.year DESC)
            FROM   staging_songs ss)
    WHERE  row_number = 1
                    """)

time_table_insert = ("""
     INSERT INTO start_time
                (start_time,
                 hour,
                 day,
                 week,
                 month,
                 year,
                 weekday)
    SELECT DISTINCT start_time,
           Extract(hour FROM start_time)  AS hour,
           Extract(day FROM start_time)   AS day,
           Extract(week FROM start_time)  AS week,
           Extract(month FROM start_time) AS month,
           Extract(year FROM start_time)  AS year,
           Extract(dow FROM start_time)   AS weekday
    FROM   (SELECT DISTINCT sp.start_time
            FROM   songplay sp)
                    """)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create]
drop_table_queries = drop_table(table_names)
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert]
