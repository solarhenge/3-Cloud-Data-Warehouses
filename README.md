# Project: Data Warehouse

## Executive Summary
Using the song and event datasets, I've created a star schema optimized for queries on song play analysis. 

### Data Wrangling Decisions

#### Staging Events Location - Empty Strings and Nulls
* After loading the staging_events table, I updated this location column by replacing spaces with nulls.
This will help with the etl logic.

#### Artists have multiple locations
* Created an artist_locations table to support this one to many relationship.
The goal is to have unique artists

#### Artist Id and Name are not unique
* An Artist Id has multiple names.
In order to handle the Artist Id not being unique for unique artist names, 
I created an artist_key column that will be used throughout the star schema to ensure the artist is unique 
and for referential integrity purposes.

#### Extra Table
* I created an extra table called song_keys which is used in the etl pipeline to determine proper song_key and artist_key column values.

## Schema for Song Play Analysis

The revised star schema is as follows:
### Fact Table
<b>1. songplays</b> - records in event data associated with song plays i.e. records with page 'NextSong'.
* songplay_key integer identity(0,1) not null,
* time_key bigint not null sortkey,
* song_key integer not null distkey,
* artist_key integer not null,
* user_key integer not null,
* level character varying not null,
* session_id integer not null,
* location character varying not null,
* user_agent character varying not null,
The total number of songplay records should equal the number of staging event records.
Expected number of records is: 6820

### Dimension Tables
<b>2. artists</b> - artists in music database
* (artist_key integer identity(0,1) not null sortkey
* ,name character varying not null

<b>3. artist_locations</b> - location of where the artist resides - artist may reside in 0, 1 or many locations
* (artist_location_key integer identity(0,1) not null sortkey
* ,artist_key integer not null 
* ,location character varying not null
* ,latitude double precision
* ,longitude double precision

<b>4. songs</b> - songs in music database - the source for the songs is from both the songs and events datasets
* (song_key integer not null sortkey
* ,song_id character varying
* ,title character varying not null
* ,artist_key integer not null
* ,year integer
* ,duration double precision

<b>5. time</b> - timestamps of records in songplays broken down into specific units. The jury is out on whether the time_key column datatype should be bigint or converted to TIMESTAMPTZ yyyy-mm-dd hh:mi:ss+/-tz. The bigint datatype is able to store the actual value from the source without any implicit conversions. I will have to ask the jury.
* (time_key bigint not null sortkey
* ,year double precision not null
* ,month double precision not null
* ,day double precision not null
* ,hour double precision not null
* ,minute double precision not null
* ,second double precision not null
* ,week double precision not null
* ,weekday double precision not null

<b>6. users</b> - users in the app
* (user_key integer not null sortkey
* ,first_name character varying not null
* ,last_name character varying not null
* ,gender character varying not null

## ETL pipeline
consists of 4 file(s):
1. create_tables.py - is where all Redshift staging and star schema tables are dropped and recreated
2. etl.py - will load S3 data into staging tables on Redshift. Staging tables are then processed to populate the star schema analytic tables on Redshift.
3. sql_queries.py - is where the sql statements reside. This file is imported into the two other files above
4. README.md - markdown file which is this file

## How To Execute
In the "Project Workspace" "Launcher" tab click on the "Terminal" square.
At the command prompt type the following in order:

python create_tables.py

python etl.py

I have put a few print statements into the code to provide some feedback on how the process is doing.

I will look at getting the Redshift data to Excel and then to Tableau for further analysis (i.e. trends etc) and question formation purposes.