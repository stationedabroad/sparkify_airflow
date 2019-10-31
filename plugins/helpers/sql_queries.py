class SqlQueries:
    """
    SQL utility helper class for staging and fact, dimension table inserts into Redshift DWH
    """

    # Data Quality Check dict. for Data Quality Check Task
    data_quality_tests = {
        "count_cubes_tables": {
            "sql": """
                SELECT
                    count(*)
                FROM
                    pg_catalog.pg_tables
                WHERE
                    schemaname = 'public'
        """, 
            "result": 6
        }

        # "test_2": {"abc": """

        # """, "result": 0
        # },

    }


    # Columns for insert into dimension and fact tables - for Facts and Dimension Tasks
    sparkify_table_columns = {
        "songplays": ["start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent"],
        "users": ["user_id", "first_name", "last_name", "gender", "level"],
        "songs": ["song_id", "title", "artist_id", "year", "duration"],
        "artists": ["artist_id", "name", "location", "latitude", "longitude"],
        "time": ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    }

    # Songplays table insert
    songplay_table_insert = ("""
        SELECT
                events.start_time,
                events.user_id, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.session_id, 
                events.location, 
                events.user_agent
                FROM (SELECT (TIMESTAMP 'epoch' + ts/1000 * interval '1 second') at time zone 'UTC' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)
    # Users table insert
    user_table_insert = ("""
        SELECT distinct user_id, first_name, last_name, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    # Song table insert
    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    # Artists table insert
    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    # Time table insert
    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    def get_formatted_columns(table):
        """
             Returns comma seperated column list of target tables
        """
        return ", ".join(SqlQueries.sparkify_table_columns.get(table))