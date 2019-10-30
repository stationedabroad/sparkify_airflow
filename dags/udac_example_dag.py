from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
# from airflow.operators.s3_to_redshift_operator import S3ToRedshiftTransfer
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 10, 30),
}

#schedule_interval='0 * * * *'
# dag = DAG('udac_example_dag',
#           default_args=default_args,
#           description='Load and transform data in Redshift with Airflow',
#           schedule_interval=None
#         )

with DAG('udac_example_dag', schedule_interval=None, default_args=default_args) as dag:
    # Task 1 - Begin
    start_operator = DummyOperator(
        task_id='Begin_execution',  
        # dag=dag
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

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table='users',
        columns="user_id, first_name, last_name, gender, level",
        sql_insert=SqlQueries.user_table_insert,
        redshift_conn_id='redshift'
    )

    start_operator >> stage_songs_to_redshift
    start_operator >> stage_events_to_redshift

# load_songplays_table = LoadFactOperator(
#     task_id='Load_songplays_fact_table',
#     dag=dag
# )

# load_user_dimension_table = LoadDimensionOperator(
#     task_id='Load_user_dim_table',
#     dag=dag
# )

# load_song_dimension_table = LoadDimensionOperator(
#     task_id='Load_song_dim_table',
#     dag=dag
# )

# load_artist_dimension_table = LoadDimensionOperator(
#     task_id='Load_artist_dim_table',
#     dag=dag
# )

# load_time_dimension_table = LoadDimensionOperator(
#     task_id='Load_time_dim_table',
#     dag=dag
# )

# run_quality_checks = DataQualityOperator(
#     task_id='Run_data_quality_checks',
#     dag=dag
# )

# end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)
