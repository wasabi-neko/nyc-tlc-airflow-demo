"""
sql_lib.py

This module creates custom Airflow operator that provide an SQL parameter interface
for executing my predefined SQLs.
"""


from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator, SnowflakeSqlApiOperator

from sql.load_taxi_zone_sql import get_sql_str as get_taxi_sqlstr
from sql.load_yellow_tripdata_external_stage_sql import get_sql_str as get_yellow_exstage_sqlstr
from sql.load_yellow_tripdata_internal_buffer_sql import get_sql_str as get_yellow_with_buffer_sqlstr
from sql.join_taxi_zone_sql import get_sql_str as get_join_sqlstr

import logging

DEFAULT_WAREHOUSE = 'COMPUTE_WH'
DEFAULT_SCHEMA = 'NYC_TLC.public'
DEFAULT_QUERY_TAG = '{"project": "nyc-tlc-demo", "type": "other"}'

class LoadTaxiOperator(SQLExecuteQueryOperator):
    def __init__(
        self,
        taxi_zone_table,
        warehouse = DEFAULT_QUERY_TAG,
        schema = DEFAULT_SCHEMA,
        query_tag = DEFAULT_QUERY_TAG,
        **kwargs
    ) -> None:
        logger = logging.getLogger(__name__)
        logger.info(get_taxi_sqlstr(taxi_zone=taxi_zone_table, warehouse=warehouse, schema=schema, query_tag=query_tag))
        super().__init__(
            sql=get_taxi_sqlstr(taxi_zone=taxi_zone_table, warehouse=warehouse, schema=schema, query_tag=query_tag),
            split_statements=True,
            **kwargs
        )


class LoadYellowExternalStage(SQLExecuteQueryOperator):
    def __init__(
        self,
        yellow_table,
        warehouse = DEFAULT_QUERY_TAG,
        schema = DEFAULT_SCHEMA,
        query_tag = DEFAULT_QUERY_TAG,
        **kwargs
    ) -> None:
        super().__init__(
            sql = get_yellow_exstage_sqlstr(
                tripdata_table = yellow_table,
                warehouse=warehouse,
                schema=schema,
                query_tag=query_tag
            ),
            split_statements=True,
            **kwargs
        )


class LoadYellowInternaBuffer(SQLExecuteQueryOperator):
    def __init__(
        self,
        yellow_table,
        warehouse = DEFAULT_QUERY_TAG,
        schema = DEFAULT_SCHEMA,
        query_tag = DEFAULT_QUERY_TAG,
        **kwargs
    ) -> None:
        super().__init__(
            sql = get_yellow_with_buffer_sqlstr(
                tripdata_table = yellow_table,
                warehouse=warehouse,
                schema=schema,
                query_tag=query_tag
            ),
            split_statements=True,
            **kwargs
        )

class JoinTaxiDripdata(SQLExecuteQueryOperator):
    def __init__(
        self,
        result,
        tripdata,
        taxi_zone,
        warehouse = DEFAULT_QUERY_TAG,
        schema = DEFAULT_SCHEMA,
        query_tag = DEFAULT_QUERY_TAG,
        **kwargs
    ) -> None:
        super().__init__(
            sql = get_join_sqlstr(
                result=result,
                tripdata=tripdata,
                warehouse=warehouse,
                taxi_zone=taxi_zone,
                schema=schema,
                query_tag=query_tag
            ),
            split_statements=True,
            **kwargs
        )
