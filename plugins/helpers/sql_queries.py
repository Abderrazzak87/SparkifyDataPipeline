class SqlQueries:    
    songplay_table_insert = ("""
        INSERT INTO songplays (  start_time, user_id, song_id, artist_id, session_id, location, user_agent )
        SELECT TIMESTAMP 'epoch' + s_event.ts/1000 * intERVAL '1 second' as start_time,
        s_event.userid,
        s_song.song_id,
        s_song.artist_id,
        s_event.sessionid,
        s_event.location,
        s_event.useragent
        FROM staging_events s_event inner join staging_songs s_song
        on s_song.artist_name = s_event.artist
        and s_song.title = s_event.song
        WHERE s_song.artist_name is not null
        and s_song.title is not null
        and s_event.page = 'NextSong'
    """)

    user_table_insert = ("""
        INSERT INTO users ( user_id, first_name, last_name, gender, level )
        SELECT DISTINCT userid,
        firstname,
        lastname,
        gender,
        level
        FROM staging_events
        WHERE userid IS NOT NULL
    """)

    song_table_insert = ("""
        INSERT INTO songs ( song_id, title, artist_id, year, duration )
        SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
        FROM staging_songs
        WHERE song_id IS NOT NULL
    """)

    artist_table_insert = ("""
        INSERT INTO artists ( artist_id, name, location, latitude, longitude )
        SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """)

    time_table_insert = ("""
        INSERT INTO time ( start_time, hour, day, week, month, year, weekday )
        SELECT DISTINCT start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
        FROM songplays
    """)

    artists_table_creat = ("""
        CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude NUMERIC,
        longitude NUMERIC);
    """)
    
    