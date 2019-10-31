from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactDimensionOperator, DataQualityOperator)
# from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Sulman M',
    'start_date': datetime(2019, 10, 30),
    'depends_on_past': False,
    'retries': 3,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
}


with DAG('sparkify_dag', schedule_interval=None, default_args=default_args) as dag:
    # Task 1 - Begin
    start_operator = DummyOperator(
        task_id='Begin_execution',
    )

    # Task 2 - Staging songs S3 to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        exec_date='{{ execution_date }}',
        table='staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song-data/A/A/A',
        json_method='auto',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credential',
        autocommit=True
    )

    # Task 3 - Staging events S3 to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        exec_date='{{ execution_date }}',
        table='staging_events',
        s3_bucket='udacity-dend',
        s3_key='log-data/2018/11/2018-11-01-events.json',
        json_method='s3://udacity-dend/log_json_path.json',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credential',
        autocommit=True
    )

    # Load songplays dimension to redshift
    load_songplays_table = LoadFactDimensionOperator(
        task_id='Load_songplays_fact_table',
        table='songplays',
        columns=SqlQueries.get_formatted_columns('songplays'),
        sql_insert=SqlQueries.songplay_table_insert,
        redshift_conn_id='redshift'
    )

    # Load users dimension to redshift
    load_user_dimension_table = LoadFactDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        columns=SqlQueries.get_formatted_columns('users'),
        sql_insert=SqlQueries.user_table_insert,
        redshift_conn_id='redshift'
    )

    # Load songs dimension to redshift
    load_song_dimension_table = LoadFactDimensionOperator(
        task_id='Load_song_dim_table',
        table='songs',
        columns=SqlQueries.get_formatted_columns('songs'),
        sql_insert=SqlQueries.song_table_insert,
        redshift_conn_id='redshift'
    )

    # Load artists dimension to redshift
    load_artist_dimension_table = LoadFactDimensionOperator(
        task_id='Load_artist_dim_table',
        table='artists',
        columns=SqlQueries.get_formatted_columns('artists'),
        sql_insert=SqlQueries.artist_table_insert,
        redshift_conn_id='redshift'
    )

    # Load time dimension to redshift
    load_time_dimension_table = LoadFactDimensionOperator(
        task_id='Load_time_dim_table',
        table='time',
        columns=SqlQueries.get_formatted_columns('time'),
        sql_insert=SqlQueries.time_table_insert,
        redshift_conn_id='redshift'
    )

    # Run data quality
    data_quality_operator = DataQualityOperator(
        task_id='Data_quality_check',
        tests=SqlQueries.data_quality_tests,
        redshift_conn_id='redshift'
    )

    # Final 1 - End
    end_operator = DummyOperator(
        task_id='End_execution',
    )

    # Dependencies for DAG
    start_operator >> stage_songs_to_redshift >> load_songplays_table
    start_operator >> stage_events_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table >> data_quality_operator
    load_songplays_table >> load_song_dimension_table >> data_quality_operator
    load_songplays_table >> load_artist_dimension_table >> data_quality_operator
    load_songplays_table >> load_time_dimension_table >> data_quality_operator >> end_operator