# Load yellow_tripdata parquet file to local stage first, then import then into yellow_table

sql_str = """
ALTER SESSION SET QUERY_TAG = {query_tag};
USE SCHEMA {schema};
USE WAREHOUSE {warehouse};

CREATE OR REPLACE TABLE {tripdata}(
    START_DATE DATE,
    PICKUP_DATETIME DATE, 
    DROPOFF_DATETIME DATE,
    PASSENGER_COUNT INT,
    TRIP_DISTANCE FLOAT,
    START_LON FLOAT,
    START_LAT FLOAT,
    END_LON FLOAT,
    END_LAT FLOAT,
    PULOCATIONID INT,
    DOLOCATIONID INT
);

CREATE OR REPLACE STAGE YELLOW_EX_STAGE
    URL = 's3://nyc-tlc-demo/trip-data/yellow'
    STORAGE_INTEGRATION = S3_INTEGRATION;

CREATE OR REPLACE STAGE YELLOW_INTERNAL_STAGE;

COPY FILES
INTO @YELLOW_INTERNAL_STAGE
FROM @YELLOW_EX_STAGE;

COPY INTO {tripdata}(
    START_DATE,
    PICKUP_DATETIME,
    DROPOFF_DATETIME,
    PASSENGER_COUNT,
    TRIP_DISTANCE,
    START_LON,
    START_LAT,
    END_LON,
    END_LAT,
    PULOCATIONID,
    DOLOCATIONID
)
FROM (
    SELECT
        -- START_DATE
        TO_DATE(REGEXP_SUBSTR(METADATA$FILENAME, '\\\\d{{4}}-\\\\d{{2}}'), 'YYYY-MM'),

        -- PICKUP_DATETIME
        TO_DATE(
            CAST(
                COALESCE(
                    GET_IGNORE_CASE($1, 'TPEP_PICKUP_DATETIME'),
                    GET_IGNORE_CASE($1, 'TRIP_PICKUP_DATETIME'),
                    GET_IGNORE_CASE($1, 'PICKUP_DATETIME')
                )
            AS VARCHAR)
        ),

        -- DROPOFF_DATETIME
        TO_DATE(
            CAST(
                COALESCE(
                    GET_IGNORE_CASE($1, 'TPEP_DROPOFF_DATETIME'),
                    GET_IGNORE_CASE($1, 'TRIP_DROPOFF_DATETIME'),
                    GET_IGNORE_CASE($1, 'DROPOFF_DATETIME')
                )
            AS VARCHAR)
        ),
        (CAST(GET_IGNORE_CASE($1, 'PASSENGER_COUNT') AS INT)),
        (CAST(GET_IGNORE_CASE($1, 'TRIP_DISTANCE') AS FLOAT)),

        -- START_LON
        (CAST(
            COALESCE(
                GET_IGNORE_CASE($1, 'START_LON'),
                GET_IGNORE_CASE($1, 'PICKUP_LONGITUDE')
            ) AS FLOAT
        )),

        -- START_LAT
        (CAST(
            COALESCE(
                GET_IGNORE_CASE($1, 'START_LAT'),
                GET_IGNORE_CASE($1, 'PICKUP_LATITUDE')
            ) AS FLOAT
        )),
        -- END_LON
        (CAST(
            COALESCE(
                GET_IGNORE_CASE($1, 'END_LON'),
                GET_IGNORE_CASE($1, 'DROPOFF_LONGITUDE')
            ) AS FLOAT
        )),
        -- END_LAT
        (CAST(
            COALESCE(
                GET_IGNORE_CASE($1, 'END_LAT'),
                GET_IGNORE_CASE($1, 'DROPOFF_LATITUDE')
            ) AS FLOAT
        )),

        -- LOCATOIN ID
        CAST(GET_IGNORE_CASE($1, 'PULOCATIONID') AS NUMBER(38,0)),
        CAST(GET_IGNORE_CASE($1, 'DOLOCATIONID') AS NUMBER(38,0))

    FROM @YELLOW_INTERNAL_STAGE
)
FILE_FORMAT = (TYPE = PARQUET);
"""


def get_sql_str(tripdata_table, query_tag, schema, warehouse):
    return sql_str.format(
        query_tag=query_tag,
        schema=schema,
        warehouse=warehouse,
        tripdata=tripdata_table
    )

if __name__ == "__main__":
    print(get_sql_str(1, 2,3,4))