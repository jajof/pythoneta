
import configparser

# CONFIG
config = configparser.ConfigParser()
config.read_file(open('configs/dwh-2.cfg'))

path_events = config.get('S3', 'log_data')
path_log_json = config.get('S3', 'LOG_JSONPATH')
path_songs = config.get('S3', 'song_data')
iam_role = config.get('IAM_ROLE', 'arn')


# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"


# CREATE STAGING TABLES #
staging_events_table_create = ("""CREATE TABLE staging_events (
                                  artist text,
                                  auth text,
                                  first_name text,
                                  gender text,
                                  itemInSession int,
                                  lastName text,
                                  lenght DOUBLE PRECISION,
                                  level text,
                                  location text,
                                  method text,
                                  page text,
                                  registration bigint,
                                  sessionId int,
                                  song text,
                                  status int,
                                  ts bigint,
                                  userAgent text,
                                  UserId int
                                )""")

staging_songs_table_create =  ("""CREATE TABLE staging_songs  (
                                  artist_id text,
                                  artist_latitude DOUBLE PRECISION,
                                  artist_location text,
                                  artist_longitude DOUBLE PRECISION,
                                  artist_name text,
                                  duration DOUBLE PRECISION,
                                  num_songs int,
                                  song_id text,
                                  title text,
                                  year int
                                )""")    

# CREATE OPERATIVE TABLES
time_table_create = ("""CREATE TABLE dim_time (
                        start_time dateTime NOT NULL PRIMARY KEY,
                        hour int NOT NULL,
                        day int NOT NULL,
                        week int NOT NULL,
                        month int NOT NULL,
                        year int NOT NULL,
                        weekday int NOT NULL
                    )""")

user_table_create = ("""CREATE TABLE dim_users (
                        user_id INT NOT NULL,
                        level TEXT NOT NULL, 
                        first_name text, 
                        last_name text, 
                        gender text,
                        PRIMARY KEY (user_id, level)
                    )""")

artist_table_create = ("""CREATE TABLE dim_artists (
                          artist_id TEXT NOT NULL PRIMARY KEY, 
                          name TEXT,  
                          location TEXT, 
                          lattitude DOUBLE PRECISION, 
                          longitude DOUBLE PRECISION
                        )""")

song_table_create = ("""CREATE TABLE dim_songs (
                        song_id text NOT NULL PRIMARY KEY, 
                        title text, 
                        artist_id TEXT NOT NULL REFERENCES dim_artists(artist_id), 
                        year int, 
                        duration DOUBLE PRECISION
                    )""")

songplay_table_create = ("""CREATE TABLE fact_songplay (
                            songplay_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, 
                            start_time DATETIME NOT NULL REFERENCES dim_time(start_time), 
                            user_id INT NOT NULL, 
                            level TEXT NOT NULL, 
                            song_id TEXT NULL REFERENCES dim_songs(song_id), 
                            artist_id TEXT NULL REFERENCES dim_artists(artist_id), 
                            session_id int NOT NULL, 
                            location TEXT, 
                            user_agent TEXT,
                            foreign key(user_id, level) references dim_users(user_id, level))
                        """)


# FILL STAGING TABLES
staging_events_copy = (f"""copy staging_events 
                           from {path_events}
                           credentials 'aws_iam_role={iam_role}'
                           json {path_log_json} 
                           region 'us-west-2';
                        """)

staging_songs_copy = (f"""copy staging_songs from {path_songs}
                           credentials 'aws_iam_role={iam_role}'
                           json 'auto ignorecase'
                           region 'us-west-2';
                        """)

# FILL FINAL TABLES
user_table_insert = ("""INSERT INTO dim_users (user_id, level, first_name, last_name, gender)
                        Select distinct
                            userid,
                            level,
                            first_name,
                            lastname,
                            gender
                        FROM staging_events
                        where userid is not NULL
""")

song_table_insert = ("""INSERT INTO dim_songs (song_id, title, artist_id, year, duration) 
                        Select distinct
                            song_id,
                            title,
                            artist_id,
                            year,
                            duration
                        FROM staging_songs
                    """)

artist_table_insert = ("""INSERT INTO dim_artists (artist_id, name, location, lattitude, longitude)
                          Select distinct
                              artist_id, 
                              artist_name,
                              artist_location,
                              artist_latitude,
                              artist_longitude
                          FROM staging_songs
                        """)

time_table_insert = ("""INSERT INTO dim_time (start_time, hour, day, week, month, year, weekday)
                        Select distinct
                            start_time,
                            EXTRACT(hour FROM start_time),
                            EXTRACT(day FROM start_time),
                            EXTRACT(week FROM start_time),
                            EXTRACT(month FROM start_time),
                            EXTRACT(year FROM start_time),
                            EXTRACT(weekday FROM start_time)
                        FROM 
                            (SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
                            FROM staging_Events
                            where page = 'NextSong') a
                                """)

songplay_table_insert = ("""INSERT INTO fact_songplay (start_time, user_id, level, song_id, artist_id, 
                                                       session_id,location,user_agent)
                            SELECT 
                                TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                                e.userid,
                                e.level,
                                s.song_id,
                                s.artist_id,
                                e.sessionid,
                                e.location,
                                e.useragent                        
                            FROM 
                                staging_events e
                                LEFT  JOIN staging_songs s on s.title = e.song and e.artist = s.artist_name
                            where page = 'NextSong';
                        """)

# number of rows queries
number_rows_staging_events = "SELECT COUNT(*) from staging_events"
number_rows_staging_songs = "SELECT COUNT(*) from staging_songs"
number_rows_time_dim = "SELECT COUNT(*) from dim_time"
number_rows_users_dim = "SELECT COUNT(*) from dim_users"
number_rows_artists_dim = "SELECT COUNT(*) from dim_artists"
number_rows_songs_dim = "SELECT COUNT(*) from dim_songs"
number_rows_songplay_fact = "SELECT COUNT(*) from fact_songplay"



# Analysis queries
question1 = "How many active users are there in the app?"
query1 = """Select count(DISTINCT USER_ID) FROM DIM_USERS"""


question2 = "What is the location with more reproductions?"
query2 = """
            Select location, count(*) 
            from fact_songplay
            group by location
            order by 2 desc
            limit 1"""


question3 = "Build a ranking with the users with more reproductions in the app"
query3 = """
            SELECT
                f.user_id, u.first_name, u.last_name, count(*)
            FROM
                fact_songplay f
                inner join dim_users u on u.user_id = f.user_id
            group by 
                f.user_id, u.first_name, u.last_name
            order by 4 desc
            limit 10

"""

question4 = "What is the date range we are working on"
query4="""
        Select
            min(start_time), max(start_time)
        from
            fact_songplay f;
"""

question5= "How many reproductions are in each hour?"
query5="""
        Select 
            Hour,
            count(*)
        from 
            fact_songplay f inner join 
            dim_time t on f.start_time = t.start_time
        group by Hour
        order by 1 asc
"""

# QUERY LIST
create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        user_table_create,  
                        artist_table_create,
                        song_table_create,
                        time_table_create,
                        songplay_table_create, 
                       ]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop
                     ]

copy_table_queries = [staging_events_copy,
                      staging_songs_copy,]

insert_table_queries = [user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert,
                        songplay_table_insert
                       ]

count_rows_queries = [number_rows_staging_events,
                      number_rows_staging_songs,
                      number_rows_time_dim,
                      number_rows_users_dim,
                      number_rows_artists_dim,
                      number_rows_songs_dim,
                      number_rows_songplay_fact]

analysis_queries = [(question1, query1),
                   (question2, query2),
                   (question3, query3),
                   (question4, query4),
                   (question5, query5)]