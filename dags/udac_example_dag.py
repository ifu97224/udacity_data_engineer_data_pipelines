from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                              LoadDimensionOperator, DataQualityOperator,
                              PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

s3_bucket="udacity-dend"
log_json_path='s3://udacity-dend/log_json_path.json'

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019,1,13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
  task_id="Create_tables",
  dag=dag,
  sql='create_tables.sql',
  postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key="log_data/{execution_date.year}/{execution_date.month}/",
    JSON_log=log_json_path,
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket=s3_bucket,
    s3_key="song_data",
    JSON_log='auto',
    provide_context=True
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    truncate_table=True,
    sql_query=SqlQueries.songplay_table_insert   
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    truncate_table=True,
    sql_query=SqlQueries.user_table_insert   
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    truncate_table=True,
    sql_query=SqlQueries.song_table_insert   
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    truncate_table=True,
    sql_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    truncate_table=True,
    sql_query=SqlQueries.time_table_insert
)

dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        {'check_sql': "SELECT (CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END) FROM songplays", 'expected_result':1},
        {'check_sql': "SELECT (CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END) FROM users", 'expected_result':1},
        {'check_sql': "SELECT (CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END) FROM songs", 'expected_result':1},
        {'check_sql': "SELECT (CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END) FROM artists", 'expected_result':1},
        {'check_sql': "SELECT (CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END) FROM time", 'expected_result':1}
    ]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift] \
>> load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] \
>> run_quality_checks >> end_operator



