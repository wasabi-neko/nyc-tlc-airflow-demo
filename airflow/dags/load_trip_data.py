import logging
from datetime import datetime, timedelta
from pprint import pp
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

SNOWFLAKE_CONN_ID = 'snowflake_default'
AWS_CONN_ID = 'aws_default'
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'anjung',
    'depends_on_past': False,
    'email': ['An.Jung@taodigitalsolutions.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'conn_id': SNOWFLAKE_CONN_ID,
    'aws_conn_id':AWS_CONN_ID 
}

@dag(
    dag_id='nyc-tlc-demo',
    default_args=default_args,
    description='load trip data from s3',
    start_date=datetime(2024, 10, 6),
    schedule='@daily',
    tags=['nyc-tlc-demo'])
def dag_gen():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with open('/opt/airflow/dags/sql/load_taxi_zone.sql', 'r') as file:
        load_taxi_sql = file.read()
    with open('/opt/airflow/dags/sql/load_yellow_tripdata.sql', 'r') as file:
        load_yellow_sql = file.read()
    with open('/opt/airflow/dags/sql/join_taxi_zone.sql', 'r') as file:
        join_taxi_sql = file.read()

    load_taxi = SQLExecuteQueryOperator(
        task_id = "load_taxi_zone_from_s3",
        sql = load_taxi_sql
    )
    load_yellow = SQLExecuteQueryOperator(
        task_id = 'load_yellow_tripdata_from_s3',
        sql = load_yellow_sql
    )
    join_taxi = SQLExecuteQueryOperator(
        task_id = 'join_taxi_zone_and_yellow',
        sql = join_taxi_sql
    )

    start >> [load_taxi, load_yellow] >> join_taxi >> end

nyc_dag = dag_gen()