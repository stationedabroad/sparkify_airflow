import datetime
import logging

from airflow import DAG
from airflow.hooks import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

start_date = datetime.datetime.utcnow() #- datetime.timedelta(hours=1.003)

pg_hook = PostgresHook(postgres_conn_id="redshift")

# def create_table():
#     sql = """
#         CREATE TABLE artists_b (
#                 artistid varchar(256) NOT NULL,
#                 name varchar(256),
#                 location varchar(256),
#                 lattitude numeric(18,0),
#                 longitude numeric(18,0)
#                 );
#       """
#     pg_hook.run(sql)

def create_table():
    pass

def insert_song():
    sql = """
    copy sparkify.staging_songs
    from 's3://udacity-dend/song-data/A/A/A/TRAAAAK128F9318786.json/staging_songs'
    json 'auto';
    """
    pg_hook.run()

def log_me():
#    start_date = datetime.datetime.utcnow()
    logging.warn(f"THis dag has started at {start_date}")
    # logging.warn(f"postgresql details {pg_hook.get_conn()}")

dag = DAG(
        "PostGresDagCheckerLocal",
        start_date=start_date
        )

start_operator = PythonOperator(
        task_id="begin_execution",
        dag=dag,
        python_callable=log_me
        )

basic_write_to_postgres = PythonOperator(
        task_id="execute_sql",
        dag=dag,
        python_callable=create_table
)

start_operator >> basic_write_to_postgres

