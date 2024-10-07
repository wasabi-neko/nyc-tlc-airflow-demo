alter session set query_tag = "{'project': 'nyc-tlc-demo', 'method': 'copy_from_external_stage'}";
use schema nyc_tlc.public;
use warehouse compute_large;


create or replace table yellow_trip_data (
    start_date date,
    pickup_datetime date, 
    dropoff_datetime date,
    passenger_count int,
    trip_distance float,
    start_lon float,
    start_lat float,
    end_lon float,
    end_lat float,
    PULOCATIONID int,
    DOLOCATIONID int
);

CREATE or REPLACE STAGE yellow_ex_stage
    URL = 's3://nyc-tlc-demo/trip-data/yellow'
    STORAGE_INTEGRATION = s3_integration;

copy into yellow_trip_data(
    start_date,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    start_lon,
    start_lat,
    end_lon,
    end_lat,
    PULOCATIONID,
    DOLOCATIONID
)
from (
    select
        -- start_date
        to_date(regexp_substr(metadata$filename, '\\d{4}-\\d{2}'), 'YYYY-MM'),

        -- pickup_datetime
        to_date(
            CAST(
                coalesce(
                    get_ignore_case($1, 'TPEP_PICKUP_DATETIME'),
                    get_ignore_case($1, 'TRIP_PICKUP_DATETIME'),
                    get_ignore_case($1, 'PICKUP_DATETIME')
                )
            AS varchar)
        ),

        -- dropoff_datetime
        to_date(
            CAST(
                coalesce(
                    get_ignore_case($1, 'TPEP_DROPOFF_DATETIME'),
                    get_ignore_case($1, 'TRIP_DROPOFF_DATETIME'),
                    get_ignore_case($1, 'DROPOFF_DATETIME')
                )
            AS varchar)
        ),
        (CAST(GET_IGNORE_CASE($1, 'PASSENGER_COUNT') AS int)),
        (CAST(GET_IGNORE_CASE($1, 'TRIP_DISTANCE') AS FLOAT)),

        -- start_lon
        (CAST(
            coalesce(
                GET_IGNORE_CASE($1, 'START_LON'),
                GET_IGNORE_CASE($1, 'PICKUP_LONGITUDE')
            ) AS FLOAT
        )),

        -- start_lat
        (CAST(
            coalesce(
                GET_IGNORE_CASE($1, 'START_LAT'),
                GET_IGNORE_CASE($1, 'PICKUP_LATITUDE')
            ) AS FLOAT
        )),
        -- end_lon
        (CAST(
            coalesce(
                GET_IGNORE_CASE($1, 'END_LON'),
                GET_IGNORE_CASE($1, 'DROPOFF_LONGITUDE')
            ) AS FLOAT
        )),
        -- end_lat
        (CAST(
            coalesce(
                GET_IGNORE_CASE($1, 'END_LAT'),
                GET_IGNORE_CASE($1, 'DROPOFF_LATITUDE')
            ) AS FLOAT
        )),

        -- locatoin id
        CAST(GET_IGNORE_CASE($1, 'PULOCATIONID') AS NUMBER(38,0)),
        CAST(GET_IGNORE_CASE($1, 'DOLOCATIONID') AS NUMBER(38,0))

    from @yellow_ex_stage
)
FILE_FORMAT = (type = parquet);