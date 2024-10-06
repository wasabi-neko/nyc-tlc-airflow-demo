
alter session set query_tag='nyc-tlc-demo';
use schema nyc_tlc.public;
use warehouse compute_large;

drop table yellow_trip_data;
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


select * from yellow_trip_data limit 1000;


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
        to_date(concat(
            coalesce(CAST(GET_IGNORE_CASE($1, 'TPEP_PICKUP_DATETIME') AS VARCHAR(16777216)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'TRIP_PICKUP_DATETIME') AS VARCHAR(16777216)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'PICKUP_DATETIME') AS VARCHAR(16777216))     , '')
        )),

        -- dropoff_datetime
        to_date(concat(
            coalesce(CAST(GET_IGNORE_CASE($1, 'TPEP_DROPOFF_DATETIME') AS VARCHAR(16777216)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'TRIP_DROPOFF_DATETIME') AS VARCHAR(16777216)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'DROPOFF_DATETIME') AS VARCHAR(16777216))     , '')
        )),
        (CAST(GET_IGNORE_CASE($1, 'PASSENGER_COUNT') AS int)),
        (CAST(GET_IGNORE_CASE($1, 'TRIP_DISTANCE') AS FLOAT)),

        -- start_lon
        coalesce(CAST(GET_IGNORE_CASE($1, 'START_LON') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'PICKUP_LONGITUDE') AS FLOAT), 0),
        -- start_lat
        coalesce(CAST(GET_IGNORE_CASE($1, 'START_LAT') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'PICKUP_LATITUDE') AS FLOAT), 0),
        -- end_lon
        coalesce(CAST(GET_IGNORE_CASE($1, 'END_LON') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'DROPOFF_LONGITUDE') AS FLOAT), 0),
        -- end_lat
        coalesce(CAST(GET_IGNORE_CASE($1, 'END_LAT') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'DROPOFF_LATITUDE') AS FLOAT), 0),

        -- locatoin id
        CAST(GET_IGNORE_CASE($1, 'PULOCATIONID') AS NUMBER(38,0)),
        CAST(GET_IGNORE_CASE($1, 'DOLOCATIONID') AS NUMBER(38,0))

    from @yellow_internal_stage
)
FILE_FORMAT = my_parquet_format;
