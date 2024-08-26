class SqlQueries:
    songplay_table_insert = ("""
        SELECT
            md5(evt.sessionid || evt.start_time) as songplay_id,
            evt.start_time, 
            evt.userid, 
            evt.level, 
            sng.song_id, 
            sng.artist_id, 
            evt.sessionid, 
            evt.location, 
            evt.useragent
        FROM (
                SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time, *
                FROM staging_events
                WHERE page='NextSong'
            ) as evt
        LEFT JOIN staging_songs as sng
            ON evt.song = sng.title
            AND evt.artist = sng.artist_name
            AND evt.length = sng.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
