import logging
from datetime import datetime, timedelta
from pprint import pp
from time import sleep
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator

SNOWFLAKE_CONN_ID = 'snowflake_default'
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'anjung',
    'depends_on_past': False,
    'email': ['An.Jung@taodigitalsolutions.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'conn_id': SNOWFLAKE_CONN_ID
}

@dag(
    dag_id='snowflake_test',
    default_args=default_args,
    description='a testing snowflake dag',
    start_date=datetime(2024, 9, 29),
    schedule='@daily',
    tags=['testing'])
def snowflake_test_api_v1():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    select_test = SQLExecuteQueryOperator(
        task_id = 'select_test',
        sql = """SELECT * FROM TEST_DB.PUBLIC.TEST_TABLE"""
    )

    start >> select_test >> end

snowflake_test = snowflake_test_api_v1()