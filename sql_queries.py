import configparser
import platform

config = configparser.ConfigParser()
read_file_name = 'dwh.cfg'
if platform.system() == 'Windows':
    read_file_name = 'D:/AWS/'+read_file_name
config.read_file(open(read_file_name))

# DROP TABLES

drop_table_artist_locations = ("""drop table if exists artist_locations cascade;""")
drop_table_artists = ("""drop table if exists artists cascade;""")
drop_table_song_keys = ("""drop table if exists song_keys cascade;""")
drop_table_songplays = ("""drop table if exists songplays cascade;""")
drop_table_songs = ("""drop table if exists songs cascade;""")
drop_table_staging_events = ("""drop table if exists staging_events cascade;""")
drop_table_staging_songs = ("""drop table if exists staging_songs cascade;""")
drop_table_time = ("""drop table if exists time cascade;""")
drop_table_users = ("""drop table if exists users cascade;""")

# CREATE TABLES

create_table_artist_locations = ("""
create table if not exists artist_locations 
(artist_location_key integer identity(0,1) not null sortkey
,artist_key integer not null 
,location character varying not null
,latitude double precision
,longitude double precision
,constraint artist_locations_pkey primary key (artist_location_key)
,constraint artist_locations_artists_fkey foreign key (artist_key)
    references artists (artist_key) match simple
);
""")

create_table_artists = ("""
create table if not exists artists 
(artist_key integer identity(0,1) not null sortkey
,name character varying not null
,constraint artists_pkey primary key (artist_key)
);
""")

create_table_song_keys = ("""
create table if not exists song_keys
(song_key integer identity(0,1) not null sortkey
,song_id character varying
,title character varying not null
,artist_id character varying
,name character varying not null
,year integer
,duration double precision
,constraint song_keys_pkey primary key (song_key)
);
""")

create_table_songplays = ("""
create table if not exists songplays
(
    songplay_key integer identity(0,1) not null,
    time_key bigint not null sortkey,
    song_key integer not null distkey,
    artist_key integer not null,
    user_key integer not null,
    level character varying not null,
    session_id integer not null,
    location character varying not null,
    user_agent character varying not null,
    constraint songplays_pkey primary key (songplay_key),
    constraint songplays_artists_fkey foreign key (artist_key)
        references artists (artist_key) match simple,
    constraint songplays_songs_fkey foreign key (song_key)
        references songs (song_key) match simple,
    constraint songplays_time_fkey foreign key (time_key)
        references "time" (time_key) match simple,
    constraint songplays_users_fkey foreign key (user_key)
        references users (user_key) match simple
);
""")

create_table_songs = ("""
create table if not exists songs 
(song_key integer not null sortkey
,song_id character varying
,title character varying not null
,artist_key integer not null
,year integer
,duration double precision
,constraint songs_pkey primary key (song_key)
);
""")

create_table_staging_events = ("""
create table if not exists staging_events
(artist text
,auth text
,firstname text
,gender text
,iteminsession text
,lastname text
,length text
,level text
,location text
,method text
,page text
,registration text
,sessionid text
,song text
,status text
,ts text
,useragent text
,userid text
);
""")

create_table_staging_songs = ("""
create table if not exists staging_songs
(num_songs text
,artist_id text
,artist_latitude text
,artist_longitude text
,artist_location text
,artist_name text
,song_id text
,title text
,duration text
,year text
);
""")

create_table_time = ("""
create table if not exists time 
(time_key bigint not null sortkey
,year double precision not null
,month double precision not null
,day double precision not null
,hour double precision not null
,minute double precision not null
,second double precision not null
,week double precision not null
,weekday double precision not null
,constraint time_pkey primary key (time_key)
);
""")

create_table_users = ("""
create table if not exists users
(user_key integer not null sortkey
,first_name character varying not null
,last_name character varying not null
,gender character varying not null
,constraint users_pkey primary key (user_key)
);
""")

# STAGING TABLES

IAM_ROLE_ARN = config.get('IAM_ROLE','ARN')
S3_LOG_DATA = config.get('S3','LOG_DATA')
S3_LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
se_source = S3_LOG_DATA
se_target = 'staging_events'
se_region = 'us-west-2'

copy_staging_events = ("""
COPY {} FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
FORMAT as JSON '{}'
""").format(se_target,se_source,IAM_ROLE_ARN,se_region,S3_LOG_JSONPATH)

S3_SONG_DATA = config.get('S3','SONG_DATA')
ss_source = S3_SONG_DATA
ss_target = 'staging_songs'
ss_region = 'us-west-2'

copy_staging_songs = ("""
COPY {} FROM '{}'
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
FORMAT as JSON 'auto'
""").format(ss_target,ss_source,IAM_ROLE_ARN,ss_region)

# FINAL TABLES

insert_into_artist_locations_1 = ("""
insert into artist_locations
(artist_key
,location
,latitude
,longitude
)
select distinct a.artist_key
,ss.artist_location as location
,cast(ss.artist_latitude as double precision) as latitude
,cast(ss.artist_longitude as double precision) as longitude
from staging_songs ss
join songs s
on ss.song_id = s.song_id
join artists a
on s.artist_key = a.artist_key
where 1 = 1
and ss.artist_location is not null
and ss.artist_latitude is not null
and ss.artist_longitude is not null
;
""")

insert_into_artist_locations_2 = ("""
insert into artist_locations
(artist_key
,location
,latitude
,longitude
)
select distinct a.artist_key
,ss.artist_location as location
,cast(ss.artist_latitude as double precision) as latitude
,cast(ss.artist_longitude as double precision) as longitude
from staging_songs ss
join songs s
on ss.song_id = s.song_id
join artists a
on s.artist_key = a.artist_key
where 1 = 1
and ss.artist_location is not null
and not exists
(select 1
from artist_locations al
where 1 = 1
and a.artist_key = al.artist_key
and ss.artist_location = al.location
and cast(ss.artist_latitude as double precision) = al.latitude
and cast(ss.artist_longitude as double precision) = al.longitude
)
;
""")

insert_into_artists = ("""
insert into artists 
(name
)
select distinct 
name
from song_keys
;
""")

insert_into_song_keys_1 = ("""
insert into song_keys 
(song_id
,title
,artist_id
,name
,year
,duration
)
select distinct 
song_id
,title
,artist_id
,artist_name
,cast(year as integer)
,cast(duration as double precision)
from staging_songs
;
""")

insert_into_song_keys_2 = ("""
insert into song_keys 
(title
,name
)
select distinct 
se.song
,se.artist
from staging_events se
where 1 = 1
and not exists
(select 1
from song_keys sk
where 1 = 1
and se.song = sk.title
and se.artist = sk.name)
and se.page = 'NextSong'
;
""")

insert_into_songplays = ("""
insert into songplays
(time_key
,song_key
,artist_key
,user_key
,level
,session_id
,location
,user_agent
)
select cast(se.ts as bigint) as time_key
,s.song_key
,s.artist_key
,u.user_key
,se.level
,cast(se.sessionid as integer) as session_id
,se.location
,se.useragent as user_agent
from staging_events se
join songs s
on se.song = s.title
join artists a
on s.artist_key = a.artist_key
and se.artist = a.name
join users u
on se.userid = u.user_key
where 1 = 1
and se.page = 'NextSong'
;
""")

insert_into_songs = ("""
insert into songs
(song_key
,song_id
,title
,artist_key
,year
,duration
)
select 
sk.song_key
,sk.song_id
,sk.title
,a.artist_key
,sk.year
,sk.duration
from song_keys sk
join artists a
on sk.name = a.name
where 1 = 1
;
""")

insert_into_time = ("""
insert into time 
(time_key
,year
,month
,day
,hour
,minute
,second
,week
,weekday
)
select distinct 
cast(ts as bigint) as time_key
,extract(year from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') as year
,extract(month from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') as month
,extract(day from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') as day
,extract(hour from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') as hour
,extract(minute from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') as minute
,extract(second from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') as second
,extract(week from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') as week
,extract(dow from timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second') as weekday
from staging_events
where 1 = 1
;
""")

insert_into_users = ("""
insert into users 
(user_key
,first_name
,last_name
,gender
)
select distinct
cast(se.userid as integer)
,se.firstname
,se.lastname
,se.gender
from staging_events se
where 1 = 1
and se.page = 'NextSong'
order by se.userid
;
""")

count_star_artist_locations = ("""select count(*) from artist_locations;""")
count_star_artists = ("""select count(*) from artists;""")
count_star_song_keys = ("""select count(*) from song_keys;""")
count_star_songplays = ("""select count(*) from songplays;""")
count_star_songs = ("""select count(*) from songs;""")
count_star_staging_events = ("""select count(*) from staging_events;""")
count_star_staging_songs = ("""select count(*) from staging_songs;""")
count_star_time = ("""select count(*) from time;""")
count_star_users = ("""select count(*) from users;""")

update_staging_songs = ("""
update staging_songs
set artist_location = Null
where artist_location = ''
;
""")

# QUERY LISTS

copy_table_queries = [copy_staging_events ,copy_staging_songs ] 
update_table_queries = [update_staging_songs ]
create_table_queries = [create_table_artists ,create_table_artist_locations ,create_table_song_keys ,create_table_songs ,create_table_staging_events ,create_table_staging_songs ,create_table_time ,create_table_users ,create_table_songplays ] 
drop_table_queries = [drop_table_artist_locations ,drop_table_artists ,drop_table_song_keys ,drop_table_songplays ,drop_table_songs ,drop_table_staging_events ,drop_table_staging_songs ,drop_table_time ,drop_table_users ] 
insert_table_queries = [insert_into_song_keys_1 ,insert_into_song_keys_2 ,insert_into_artists ,insert_into_songs ,insert_into_time ,insert_into_users ,insert_into_songplays ,insert_into_artist_locations_1 ,insert_into_artist_locations_2  ] 
counting_stars_queries = [count_star_artist_locations ,count_star_artists ,count_star_song_keys ,count_star_songplays ,count_star_songs ,count_star_staging_events ,count_star_staging_songs, count_star_time ,count_star_users ]
