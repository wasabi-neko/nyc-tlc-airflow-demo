"""
snowflake_test.py

Test the Snowflake connection with Airflow by using simple snowflake query operator to
run sql command: `SELECT * FROM snowflake.account_usage.databases LIMIT 1;`
"""

import logging
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator

SNOWFLAKE_CONN_ID = 'snowflake_default'
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'anjung',
    'depends_on_past': False,
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
        sql = """
        SELECT * FROM snowflake.account_usage.databases LIMIT 1;
        SELECT * FROM snowflake.account_usage.databases LIMIT 2;
        SELECT * FROM snowflake.account_usage.databases LIMIT 3;
        """,
        split_statements=True,
        # statement_count=3
    )

    start >> select_test >> end

snowflake_test = snowflake_test_api_v1()