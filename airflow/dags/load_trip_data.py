import json
import logging
from datetime import datetime, timedelta
from pprint import pp
from airflow.decorators import task, dag
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator 
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from sql.sql_lib import LoadTaxiOperator, LoadYellowExternalStage, LoadYellowInternaBuffer, JoinTaxiDripdata

SNOWFLAKE_CONN_ID = 'snowflake_default'
AWS_CONN_ID = 'aws_default'
logger = logging.getLogger(__name__)

def get_sql(file_name: str) -> str:
    with open('/opt/airflow/dags/sql/' + file_name, 'r') as file:
        sql_str = file.read()
    return sql_str

default_args = {
    'owner': 'anjung',
    'email': ['An.Jung@taodigitalsolutions.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'conn_id': SNOWFLAKE_CONN_ID,
    'aws_conn_id':AWS_CONN_ID 
}

@dag(
    dag_id='nyc-tlc-demo-compare-v2',
    default_args=default_args,
    description='load trip data from s3',
    # start_date=datetime(2024, 10, 6),
    schedule=None,
    max_active_runs=1,  # allow only one active run at a time
    catchup=False,
    tags=['nyc-tlc-demo'])
def dag_gen():
    # Prepare parameters
    compute_xs = "COMPUTE_WH"
    compute_large = "COMPUTE_XL"
    in_compute_xl = "in_compute_xl"
    ex_compute_xl = "ex_compute_xl"

    project_name = "nyc-tlc-demo-compare-v2"

    ex_taxi_zone = "ex_taxi_zone"
    ex_yellow = "ex_yellow_tripdata"
    ex_final = "ex_final"

    in_taxi_zone = "in_taxi_zone"
    in_yellow = "in_yellow_tripdata"
    in_final = "in_final"

    # Tasks
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id="generate_unique_query_tag", multiple_outputs=True)
    def generate_unique_query_tag(project_name, **kwargs):
        dag_time_stamp = datetime.now().timestamp()
        def _get_query_tag_str(project_name, _type):
            return "'" + json.dumps(
                {
                    "project": project_name,
                    "type": _type,
                    "dag_timestamp": dag_time_stamp
                }
            ) + "'"

        return {
            "ex_query_tag": _get_query_tag_str(project_name, "external_stage"),
            "in_query_tag": _get_query_tag_str(project_name, "internal_stage"),
            "other_query_tag": _get_query_tag_str(project_name, "other"),
        }

    tag_gen = generate_unique_query_tag(project_name)


    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        sql=f"""
            ALTER SESSION SET QUERY_TAG = {tag_gen['other_query_tag']};
            USE SCHEMA NYC_TLC.PUBLIC;
            USE WAREHOUSE {compute_xs};
            DROP table if exists {ex_taxi_zone};
            DROP table if exists {ex_yellow};
            DROP table if exists {ex_final};
            DROP table if exists {in_taxi_zone};
            DROP table if exists {in_yellow};
            DROP table if exists {in_final};
        """,
    )

    ex_load_taxi = LoadTaxiOperator(
        task_id = "ex_load_taxi_zone_from_s3",
        taxi_zone_table=ex_taxi_zone,
        query_tag=tag_gen['ex_query_tag'],
        warehouse=ex_compute_xl,
    )
    ex_load_yellow = LoadYellowExternalStage(
        task_id = 'ex_load_yellow_tripdata_from_s3',
        yellow_table=ex_yellow,
        query_tag=tag_gen['ex_query_tag'],
        warehouse=ex_compute_xl,
    )
    ex_join_taxi = JoinTaxiDripdata(
        task_id = 'ex_join_taxi_zone_and_yellow',
        result=ex_final,
        tripdata=ex_yellow,
        taxi_zone=ex_taxi_zone,
        query_tag=tag_gen['ex_query_tag'],
        warehouse=ex_compute_xl
    )

    in_load_taxi = LoadTaxiOperator(
        task_id = "in_load_taxi_zone_from_s3",
        taxi_zone_table=in_taxi_zone,
        query_tag=tag_gen['in_query_tag'],
        warehouse=in_compute_xl,
    )
    in_load_yellow = LoadYellowInternaBuffer(
        task_id = 'in_load_yellow_tripdata_from_s3',
        yellow_table=in_yellow,
        query_tag=tag_gen['in_query_tag'],
        warehouse=in_compute_xl,
    )
    in_join_taxi = JoinTaxiDripdata(
        task_id = 'in_join_taxi_zone_and_yellow',
        result=in_final,
        tripdata=in_yellow,
        taxi_zone=in_taxi_zone,
        query_tag=tag_gen['in_query_tag'],
        warehouse=in_compute_xl
    )

    start >> tag_gen >> drop_table
    drop_table >> [ex_load_taxi, ex_load_yellow] >> ex_join_taxi >> end
    drop_table >> [in_load_taxi, in_load_yellow] >> in_join_taxi >> end

nyc_dag = dag_gen()