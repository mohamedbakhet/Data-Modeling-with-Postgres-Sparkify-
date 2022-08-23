# DROP TABLES

songplay_table_drop = "drop table IF EXISTS songplays;"
user_table_drop = "drop table IF EXISTS users;"
song_table_drop = "drop table IF EXISTS songs;"
artist_table_drop = "drop table IF EXISTS artists;"
time_table_drop = "drop table IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("create table if not exists songplays (\
  songplay_id serial primary key, \
  start_time TIMESTAMP NOT NULL, \
  user_id int NOT NULL, \
  level varchar(5), \
  song_id varchar, \
  artist_id varchar,\ 
  session_id varchar NOT NULL,\ 
  location varchar, \
  user_agent varchar\
);")

user_table_create = ("create table if not exists users (\
  user_id int primary key, \
  first_name varchar, \
  last_name varchar, \
  gender char, \
  level varchar(5)\
);")

song_table_create = ("create table if not exists songs (\
  song_id varchar primary key, title varchar NOT NULL, \
  artist_id varchar NOT NULL, year int, \
  duration float NOT NULL\
);")

artist_table_create = ("create table if not exists artists (\
    artist_id  varchar primary key,\
    name varchar NOT NULL,\
    location varchar,\
    latitude float,\
    longitude float\
);")

time_table_create = ("create table if not exists time (\
  start_time TIMESTAMP PRIMARY KEY,\
  hour int, \
  day int,\
  week int,\
  month int,\
  year int, \
  weekday varchar\
);")

# INSERT RECORDS

songplay_table_insert = ("insert into songplays(\
    start_time, user_id, level, song_id, artist_id , \
    session_id, location, user_agent)\ 
    values\
    (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING;")

user_table_insert = ("insert into users (\
      user_id, first_name, last_name, gender,level \
    ) \
    values\
    (% s,% s,% s,% s,% s) ON CONFLICT (user_id) DO NOTHING;")

song_table_insert = ("insert into songs (\
      song_id, title, artist_id, year, duration\
    ) \
    values\ 
      (% s,% s,% s,% s,% s) ON CONFLICT (song_id) DO NOTHING;")

artist_table_insert = ("insert into artists (\
      artist_id, name, location, latitude, \
      longitude\
    ) \
    values\ 
      (% s,% s,% s,% s,% s) ON CONFLICT (artist_id) DO NOTHING;")


time_table_insert = ("insert into time (\
      start_time, hour, day, week, month, \
      year, weekday\
    ) \
    values \
      (% s,% s,% s,% s,% s,% s,% s) ON CONFLICT (start_time) DO NOTHING;")

# FIND SONGS
song_select = ("select \
  songs.song_id, \
  songs.artist_id\ 
from \
  songs \
  join artists on songs.artist_id = artists.artist_id \
where \
  songs.title =% s\ 
  and artists.name =% s\ 
  and songs.duration =% s;")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]