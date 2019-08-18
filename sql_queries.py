import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create= ("""
  CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR(1),
    item_in_session INT,
    last_name VARCHAR,
    length DECIMAL(10, 5),
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration REAL,
    session_id BIGINT,
    song VARCHAR,
    status INT,
    ts BIGINT,
    user_agent VARCHAR,
    user_id BIGINT
    );
""")

staging_songs_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude REAL,
    artist_longitude REAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration DECIMAL(10, 5),
    year INT
    );
""")

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays (
      songplay_id BIGINT IDENTITY(1, 1) NOT NULL,
      start_time BIGINT NOT NULL SORTKEY,
      user_id INT NOT NULL,
      level VARCHAR,
      song_id VARCHAR,
      artist_id VARCHAR DISTKEY,
      session_id INT,
      location VARCHAR,
      user_agent VARCHAR
    );
""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users (
    user_id INT NOT NULL,
    first_name VARCHAR,
    last_name VARCHAR,
    gender VARCHAR(1),
    level VARCHAR
    );
""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs (
      song_id VARCHAR NOT NULL,
      title VARCHAR NOT NULL,
      artist_id VARCHAR DISTKEY,
      year INT,
      duration DECIMAL(10,5)
    );
""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL DISTKEY,
    name VARCHAR NOT NULL,
    location VARCHAR,
    latitude REAL,
    longitude REAL
    );
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS times (
  start_time BIGINT NOT NULL SORTKEY,
  hour INT NOT NULL,
  day INT NOT NULL,
  week INT NOT NULL,
  month INT NOT NULL,
  year INT NOT NULL,
  weekday INT NOT NULL
  );
""")

# STAGING TABLES

staging_events_copy = ("""
  COPY staging_events
  FROM {}
  credentials 'aws_iam_role={}'
  JSON AS {}
  region 'us-west-2';
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
  COPY staging_songs
  FROM {}
  credentials 'aws_iam_role={}'
  JSON AS 'auto' TRUNCATECOLUMNS
  region 'us-west-2'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
  SELECT
    staging_events.ts,
    staging_events.user_id,
    staging_events.level,
    staging_songs.song_id,
    staging_songs.artist_id,
    staging_events.session_id,
    staging_events.location,
    staging_events.user_agent
  FROM
    staging_events
    LEFT JOIN staging_songs
    ON staging_events.song = staging_songs.title
      AND staging_events.artist = staging_songs.artist_name
      AND ROUND(staging_events.length) = ROUND(staging_songs.duration)
  WHERE
    staging_events.page = 'NextSong'
  ;
""")

user_table_insert = ("""
  INSERT INTO users (user_id, first_name, last_name, gender,level)
  SELECT user_id, first_name, last_name, gender, level
  FROM (
    SELECT user_id, first_name, last_name, gender, level,
      RANK() OVER (PARTITION BY user_id ORDER BY ts DESC) AS ts_rank
    FROM staging_events
  )
  WHERE ts_rank = 1
    AND user_id IS NOT NULL
  ;
""")

song_table_insert = ("""
  INSERT INTO songs (song_id, title, artist_id, year, duration)
  SELECT song_id, title, artist_id, year, duration
  FROM staging_songs
""")

artist_table_insert = ("""
  INSERT INTO artists (artist_id, name, location, latitude, longitude)
  SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
  FROM staging_songs
""")

time_table_insert = ("""
  INSERT INTO times (start_time, hour, day, week, month, year, weekday)
  SELECT DISTINCT
    ts,
    EXTRACT(hour FROM (TIMESTAMP 'epoch' + ts * interval '0.001 seconds')),
    EXTRACT(day FROM (TIMESTAMP 'epoch' + ts * interval '0.001 seconds')),
    EXTRACT(week FROM (TIMESTAMP 'epoch' + ts * interval '0.001 seconds')),
    EXTRACT(month FROM (TIMESTAMP 'epoch' + ts * interval '0.001 seconds')),
    EXTRACT(year FROM (TIMESTAMP 'epoch' + ts * interval '0.001 seconds')),
    EXTRACT(dayofweek FROM (TIMESTAMP 'epoch' + ts * interval '0.001 seconds'))
  FROM staging_events
  ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
