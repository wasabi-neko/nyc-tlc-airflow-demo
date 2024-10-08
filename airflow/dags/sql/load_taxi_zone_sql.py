# load taxi_zone file from s3 to snowflake

sql_str = """
ALTER SESSION SET QUERY_TAG = {query_tag};
USE SCHEMA {schema};
USE WAREHOUSE {warehouse};

CREATE OR REPLACE FILE FORMAT CSV_HEADER_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

CREATE OR REPLACE TABLE {taxi_table}(
    X FLOAT,
    Y FLOAT,
    ZONE VARCHAR(256),
    LOCATIONID INT,
    BOROUGH VARCHAR(256)
);

CREATE OR REPLACE STAGE TAXI_ZONE_STAGE
    URL = 's3://nyc-tlc-demo/taxi_zones.csv'
    STORAGE_INTEGRATION = S3_INTEGRATION
    FILE_FORMAT = CSV_HEADER_FORMAT;

COPY INTO {taxi_table}
FROM @TAXI_ZONE_STAGE;

DROP STAGE TAXI_ZONE_STAGE;
"""

def get_sql_str(taxi_zone, query_tag, schema, warehouse):
    return sql_str.format(
        query_tag=query_tag,
        schema=schema,
        warehouse=warehouse,
        taxi_table=taxi_zone
    )
