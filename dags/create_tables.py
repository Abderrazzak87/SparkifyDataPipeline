import logging
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from datetime import datetime

from airflow import DAG
from helpers import SqlQueries

default_args = {
    'owner': 'Abderrazzak'
}

drop_table_sql = "DROP TABLE IF EXISTS public.{};"

dag = DAG('create_dwh_spakify_tables',
          default_args=default_args,
          description='Creates Sparkify Redshift tables',
          start_date=datetime(2020, 6, 11),
          schedule_interval=None
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_songplays = PostgresOperator(
     task_id="drop_songplays_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql=(drop_table_sql).format("songplays")
 )

creat_songplays = PostgresOperator(
     task_id="creat_songplays_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql=""" CREATE TABLE IF NOT EXISTS songplays (
        songplay_id int not null IDENTITY(0,1) PRIMARY KEY, 
        start_time timestamp NOT NULL , 
        user_id int  , 
        song_id varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        session_id int, 
        location varchar, 
        user_agent varchar
    ) DISTKEY("song_id") SORTKEY("start_time")"""
 )

drop_staging_events = PostgresOperator(
     task_id="drop_staging_events_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql=(drop_table_sql).format("staging_events")
 )

creat_staging_events = PostgresOperator(
     task_id="creat_staging_events_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql="""CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char(1),
        itemInSession int,
        lastName varchar,
        length numeric,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration numeric,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userId int
    ) DISTKEY("song") SORTKEY("ts")"""
 )



drop_staging_songs = PostgresOperator(
     task_id="drop_staging_songs_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql=(drop_table_sql).format("staging_songs")
 )

creat_staging_songs = PostgresOperator(
     task_id="creat_staging_songs_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql="""CREATE TABLE IF NOT EXISTS staging_songs (
        artist_id varchar, 
        artist_latitude numeric, 
        artist_location varchar, 
        artist_longitude numeric,
        artist_name varchar,
        duration numeric,
        num_songs int,
        song_id varchar,
        title varchar,
        year int
    ) DISTKEY("title")"""
 )

drop_artists = PostgresOperator(
     task_id="drop_artists_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql=(drop_table_sql).format("artists")
 )

creat_artists = PostgresOperator(
     task_id="creat_artists_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql="""CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar not null  PRIMARY KEY, 
        name varchar distkey, 
        location varchar, 
        latitude numeric, 
        longitude numeric
    ) SORTKEY("artist_id")"""
 )

drop_users = PostgresOperator(
     task_id="drop_users_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql=(drop_table_sql).format("users")
 )

creat_users = PostgresOperator(
     task_id="creat_users_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql="""CREATE TABLE IF NOT EXISTS users (
        user_id int not null  PRIMARY KEY, 
        first_name varchar NOT NULL, 
        last_name varchar NOT NULL, 
        gender char(1) NOT NULL, 
        level varchar NOT NULL
    ) SORTKEY("user_id")"""
 )

drop_songs = PostgresOperator(
     task_id="drop_songs_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql=(drop_table_sql).format("songs")
 )


creat_songs = PostgresOperator(
     task_id="creat_songs_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql="""CREATE TABLE IF NOT EXISTS songs (
        song_id varchar not null  PRIMARY KEY, 
        title varchar NOT NULL, 
        artist_id varchar NOT NULL, 
        year int NOT NULL, 
        duration numeric NOT NULL
    ) SORTKEY("song_id")"""
 )

drop_time = PostgresOperator(
     task_id="drop_time_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql=(drop_table_sql).format("time")
 )

creat_time = PostgresOperator(
     task_id="creat_time_table",
     dag=dag,
     postgres_conn_id="redshift",
     sql="""CREATE TABLE IF NOT EXISTS time (
        start_time timestamp not null  PRIMARY KEY, 
        hour int NOT NULL, 
        day int NOT NULL, 
        week int NOT NULL, 
        month int NOT NULL, 
        year int NOT NULL, 
        weekday int NOT NULL
    ) SORTKEY("start_time")"""
 )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies
start_operator >> drop_songplays >> creat_songplays >> end_operator
start_operator >> drop_staging_events >> creat_staging_events >> end_operator
start_operator >> drop_staging_songs >> creat_staging_songs >> end_operator
start_operator >> drop_artists >> creat_artists >> end_operator
start_operator >> drop_users >> creat_users >> end_operator
start_operator >> drop_songs >> creat_songs >> end_operator
start_operator >> drop_time >> creat_time >> end_operator



