"""
aws_s3_test.py

Test the aws s3 connection with Ariflow by running simple S3 list operator
"""


import logging
from datetime import datetime, timedelta
from pprint import pp
from time import sleep
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

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
    'aws_conn_id':AWS_CONN_ID 
}

@dag(
    dag_id='aws_s3_test',
    default_args=default_args,
    description='a testing snowflake dag',
    start_date=datetime(2024, 9, 29),
    schedule='@daily',
    tags=['testing'])
def aws_s3_test_api_v1():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    list_prefixes = S3ListOperator(
        task_id="list_prefixes",
        bucket='nyc-tlc-demo'
    )


    start >> list_prefixes >> end

dag = aws_s3_test_api_v1()