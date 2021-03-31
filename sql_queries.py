
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
accident_staging_table_drop = "DROP TABLE IF EXISTS accident_staging_table"
city_staging_table_drop = "DROP TABLE IF EXISTS city_staging_table"
covid_staging_table_drop = "DROP TABLE IF EXISTS covid_staging_table"
# user_table_drop = "DROP TABLE IF EXISTS user_table"
# song_table_drop = "DROP TABLE IF EXISTS song_table"
# artist_table_drop = "DROP TABLE IF EXISTS artist_table"
# time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE STAGING TABLES 
create_accident_staging_table = ("""
    CREATE TABLE IF NOT EXISTS accident_staging_table (
        id                      VARCHAR,
        source                  VARCHAR,
        tmc                     REAL,
        severity                INT,
        start_time              TIMESTAMP,
        end_time                TIMESTAMP,
        start_lat               NUMERIC(9,6),
        start_lng               NUMERIC(9,6),
        end_lat                 NUMERIC(9,6),
        end_lng                 NUMERIC(9,6),
        distance                REAL,
        description             VARCHAR(MAX),
        number                  REAL,
        street                  VARCHAR,
        side                    VARCHAR(1),
        city                    VARCHAR,
        county                  VARCHAR,
        state                   VARCHAR(5),
        zipcode                 VARCHAR,
        country                 VARCHAR,
        timezone                VARCHAR,
        airport_code            VARCHAR,
        weather_time            TIMESTAMP,
        temprature              REAL,
        wind_chill              REAL,
        humidity                REAL,
        pressure                REAL,
        visibility              REAL,
        wind_direction          VARCHAR,
        wind_speed              REAL,
        percipiration           REAL,
        weather_condition       VARCHAR,
        amenity                 BOOLEAN,          
        bump                    BOOLEAN,
        crossing                BOOLEAN,
        give_way                BOOLEAN,
        junction                BOOLEAN,
        no_exit                 BOOLEAN,
        railway                 BOOLEAN,
        roundabout              BOOLEAN,
        station                 BOOLEAN,
        stop                    BOOLEAN,
        traffic_calming         BOOLEAN,
        traffic_signal          BOOLEAN,
        turning_loop            BOOLEAN,
        sunrise_sunset          VARCHAR,
        civil_twilight          VARCHAR,
        nautical_twilight       VARCHAR,
        astronomical_twilight   VARCHAR
    )
""")

create_city_staging_table = ("""
    CREATE TABLE IF NOT EXISTS city_staging_table (
        city                   VARCHAR,
        state                  VARCHAR,
        race                   VARCHAR,
        count                  INT,
        median_age             REAL,
        male_population        INT,
        female_populaiton      INT,
        total_population       INT,
        number_of_veterans     INT,
        foreign_born           INT,
        average_household_size REAL,
        state_code             VARCHAR
    )
""")

create_covid_staging_table = ("""
    CREATE TABLE IF NOT EXISTS covid_staging_table (
        country_name        VARCHAR,
        day                 TIMESTAMP,
        stringency_index    REAL,
        total_vaccinations  REAL,
        total_deaths        REAL,
        total_cases         REAL,
        daily_new_cases     REAL,
        biweekly_cases      REAL
    )
""")


# CREATE FACT/DIMENSION TABLE
    # accident fact table
    # time table
    # city table
    # covid case by day table


# DEFINE FACT & DIMESNTION TABLE SCEHMA

# Must add primary key references
create_accident_fact_table = ("""
    CREATE TABLE IF NOT EXISTS accident_staging_table (
        id              VARCHAR,
        tmc             REAL,
        severity        INT,
        start_time      VARCHAR(1),
        end_time        INT,
        description     VARCHAR,
        city            VARCHAR,
        state           VARCHAR(5),
        zipcode         INT,
        temprature      REAL,
        wind_chill      REAL,
        humidity        REAL,
        pressure        REAL,
        visibility      REAL,
        wind_speed      REAL,
        wind_condition  VARCHAR,
        sunrise_sunset  VARCHAR,
    )
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table (
        songplay_key INT IDENTITY(0,1) PRIMARY KEY,
        time_key     TIMESTAMP REFERENCES time_table (time_key) NOT NULL,
        user_key     BIGINT REFERENCES user_table (user_key) NOT NULL,
        level        VARCHAR(15),
        song_key     VARCHAR REFERENCES song_table (song_key) NOT NULL DISTKEY SORTKEY ,
        artist_key   VARCHAR(20) REFERENCES artist_table (artist_key) NOT NULL,
        session_id   BIGINT,
        location     VARCHAR,
        userAgent    VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table(
        user_key    INT PRIMARY KEY,
        user_id     INT SORTKEY,
        firstName   VARCHAR,
        lastName    VARCHAR,
        gender      VARCHAR(1),
        level       VARCHAR(15)        
    )DISTSTYLE ALL
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table(
        song_key    VARCHAR PRIMARY KEY,
        song_id     VARCHAR DISTKEY SORTKEY,
        title       VARCHAR,
        artist_id   VARCHAR(20),
        year        INT,        
        duration    REAL   
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table(
        artist_key       VARCHAR(20) PRIMARY KEY,
        artist_id        VARCHAR(20) SORTKEY,
        artist_name      VARCHAR,
        location         VARCHAR,
        artist_latitude  NUMERIC(8,6),
        artist_longitude NUMERIC(9,6)        
    )DISTSTYLE ALL
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table(
        time_key    TIMESTAMP PRIMARY KEY,
        start_time  TIMESTAMP SORTKEY,
        hour        INT,
        day         INT,
        week        INT,
        month       INT,
        year        INT,
        weekday     BOOLEAN 
    )DISTSTYLE ALL
""")

# STAGING TABLES
staging_accident_copy = ("""
    COPY accident_staging_table FROM {}
    iam_role {}
    CSV
    IGNOREHEADER 1
    REGION 'us-west-2'
""").format(config['S3']['ACCIDENT_DATA'], 
            config['IAM_ROLE']['ARN'])

staging_city_copy = ("""
    COPY city_staging_table FROM {}
    iam_role {}
    JSON 'auto'
    REGION 'us-west-2'
""").format(config['S3']['CITY_DATA'], 
            config['IAM_ROLE']['ARN'])

staging_covid_copy = ("""
    COPY covid_staging_table FROM {}
    iam_role {}
    CSV
    IGNOREHEADER 1
    REGION 'us-west-2'
""").format(config['S3']['COVID_DATA'], 
            config['IAM_ROLE']['ARN'])


# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplay_table (time_key, user_key, level, song_key, 
                                artist_key, session_id, location, userAgent)
    SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second', 
           userId, level, song_id, artist_id, sessionId, location, userAgent
    FROM staging_events_table events
    JOIN staging_songs_table songs
    ON songs.title = events.song AND songs.artist_name = events.artist 
""")

user_table_insert = ("""
    INSERT INTO user_table (user_key, user_id, firstName, lastName, gender, level)
    SELECT DISTINCT userId, userId, firstName, lastName, gender, level
    FROM staging_events_table
    WHERE userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO song_table (song_key, song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, song_id, title, artist_id, year, duration
    FROM staging_songs_table
""")

artist_table_insert = ("""
    INSERT INTO artist_table (artist_key, artist_id, artist_name, location, 
                            artist_latitude, artist_longitude)
    SELECT DISTINCT artist_id, artist_id, artist_name, artist_location, 
           artist_latitude, artist_longitude 
    FROM staging_songs_table
""")

time_table_insert = ("""
    INSERT INTO time_table (time_key, start_time, hour, day,
                            week, month, year, weekday)
    SELECT DISTINCT a.start_time,
           a.start_time,
           EXTRACT(hour FROM a.start_time),
           EXTRACT(day FROM a.start_time),
           EXTRACT(week FROM a.start_time),
           EXTRACT(month FROM a.start_time),
           EXTRACT(year FROM a.start_time),
           CASE
               WHEN 5 = EXTRACT(weekday FROM a.start_time) THEN True
               WHEN 6 = EXTRACT(weekday FROM a.start_time) THEN True
               ELSE False
           END
    FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time 
    FROM staging_events_table)a;
""")

# QUERY LISTS
create_table_queries = [create_accident_staging_table, create_city_staging_table, create_covid_staging_table]
drop_table_queries = [accident_staging_table_drop, city_staging_table_drop, covid_staging_table_drop]

copy_table_queries = [staging_accident_copy, staging_covid_copy, staging_city_copy]
insert_table_queries = []
