import random
import logging
from datetime import datetime, timedelta
from pprint import pp
from time import sleep
from airflow.decorators import task, dag

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'anjung',
    'depends_on_past': False,
    'email': ['An.Jung@taodigitalsolutions.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id='first_dag',
    default_args=default_args,
    description='this is a dag',
    start_date=datetime(2024, 9, 29),
    schedule= timedelta(seconds=30),
    tags=['testing'])
def first_dag_generator():

    @task(task_id="print_the_context")
    def python_fun1(ds=None, **kwargs):
        pp(kwargs)
        print(ds)
        logger.info("print the context")
        logger.info(kwargs)
        return "my text: return from python fun1"

    @task(task_id="sleep_5")
    def sleep_fun2(**kwargs):
        logger.info("in sleep")
        sleep(10)

        r = random.randint(0, 10)
        if r > 9:
            logger.error("random error!!")
            raise Exception("error")

        return r

    @task(task_id="print_random")
    def python_print_random(r, **context):
        logger.info("my num:" + str(r))
        pp(context)
        print("not random text here")
        return "my text: return from python_print_random" + str(r)
    

    t1 = python_fun1()
    t2 = sleep_fun2()
    t3 = python_print_random(r=t2)

    t1 >> t2 >> t3

first_dag = first_dag_generator() # run the dag
