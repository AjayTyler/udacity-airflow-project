from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from airflow.models import Variable
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        source_data=f'{Variable.get("s3_bucket")}/{Variable.get("s3_log_data")}',
        destination_table='public.staging_events',
        json_format=f's3://{Variable.get("s3_bucket")}/log_json_path.json',
        region=f'{Variable.get("s3_region")}'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        source_data = f'{Variable.get("s3_bucket")}/{Variable.get("s3_song_data")}',
        destination_table = 'public.staging_songs',
        json_format='auto',
        region=f'{Variable.get("s3_region")}'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        destination_table='public.songplays',
        primary_key='songplay_id',
        insert_mode='insert',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        destination_table='public.users',
        primary_key='userid',
        insert_mode='merge',
        sql=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        destination_table='public.songs',
        primary_key='songid',
        insert_mode='merge',
        sql=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        destination_table='public.artists',
        primary_key='artistid',
        insert_mode='merge',
        sql=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        destination_table='public.time',
        primary_key='start_time',
        insert_mode='append_new',
        sql=SqlQueries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tests={
            'empty_table_check': ['artists', 'songplays', 'songs', 'time', 'users'],
            'freshness_check': [
                {
                    'table': 'songplays',
                    'target_column': 'start_time'
                }
            ]
        }
    )

    end_operator = DummyOperator(task_id='End_execution')

    # Our staging runs in parallel
    start_operator >> [
        stage_events_to_redshift,
        stage_songs_to_redshift
    ]

    # Songplays is loaded after the dependent data is staged
    load_songplays_table << [
        stage_events_to_redshift,
        stage_songs_to_redshift
    ]

    # The dimension jobs run after the songplays table is loaded
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]

    # After all of that has finished, we run the quality check
    run_quality_checks << [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]

    # Finally, we make our DAG look nice by tying it all together
    end_operator << run_quality_checks

final_project_dag = final_project()
