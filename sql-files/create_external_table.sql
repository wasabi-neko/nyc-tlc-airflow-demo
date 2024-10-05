alter session set query_tag='nyc-tlc-demo-extable';

-- Create external table with INFER_SCHEMA
create external table all_ex_table
    using template (
        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(
            INFER_SCHEMA(
                LOCATION=>'@s3_stage',
                FILE_FORMAT=>'my_parquet_format',
                IGNORE_CASE => TRUE
            )
        )
    )
    location = @s3_stage,
    file_format = my_parquet_format
    auto_refresh = false;

-- Check the DDL
SELECT GET_DDL('TABLE', 'all_ex_table');

-- After reading the DDL from auto-generated schema,
-- Create the our final external table with conditions
create or replace external table super_extable(
--  not  sure the mapping of the vendor id
    START_DATE date AS to_date(substr(metadata$filename, 17, 7), 'YYYY-MM'),
	VENDOR_ID VARCHAR(256) AS (
        concat(
            coalesce(CAST(GET_IGNORE_CASE($1, 'VENDOR_ID') AS VARCHAR(256)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'VENDORID') AS varchar(256)), '')
        )
    ),  
	VENDOR_NAME VARCHAR(16777216) AS (CAST(GET_IGNORE_CASE($1, 'VENDOR_NAME') AS VARCHAR(16777216))),

	TPEP_PICKUP_DATETIME VARCHAR(16777216) AS (
        concat(
            coalesce(CAST(GET_IGNORE_CASE($1, 'TPEP_PICKUP_DATETIME') AS VARCHAR(16777216)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'TRIP_PICKUP_DATETIME') AS VARCHAR(16777216)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'PICKUP_DATETIME') AS VARCHAR(16777216))     , '')
        )
    ),

	TPEP_DROPOFF_DATETIME VARCHAR(16777216) AS (
        concat(
            coalesce(CAST(GET_IGNORE_CASE($1, 'TPEP_DROPOFF_DATETIME') AS VARCHAR(16777216)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'TRIP_DROPOFF_DATETIME') AS VARCHAR(16777216)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'DROPOFF_DATETIME') AS VARCHAR(16777216))     , '')
        )
    ),

	PASSENGER_COUNT int AS (CAST(GET_IGNORE_CASE($1, 'PASSENGER_COUNT') AS int)),
	TRIP_DISTANCE FLOAT AS (CAST(GET_IGNORE_CASE($1, 'TRIP_DISTANCE') AS FLOAT)),

	STORE_AND_FWD_FLAG VARCHAR(256) AS (
        concat(
            coalesce(CAST(GET_IGNORE_CASE($1, 'STORE_AND_FWD_FLAG') AS VARCHAR(256)), ''),
            coalesce(CAST(GET_IGNORE_CASE($1, 'STORE_AND_FORWARD') AS VARCHAR(256)), '')
        )
    ),
 
--  locations
	START_LON FLOAT AS (
        coalesce(CAST(GET_IGNORE_CASE($1, 'START_LON') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'PICKUP_LONGITUDE') AS FLOAT), 0)
    ),

    START_LAT FLOAT AS (
        coalesce(CAST(GET_IGNORE_CASE($1, 'START_LAT') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'PICKUP_LATITUDE') AS FLOAT), 0)
    ),

	END_LON FLOAT AS (
        coalesce(CAST(GET_IGNORE_CASE($1, 'END_LON') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'DROPOFF_LONGITUDE') AS FLOAT), 0)
    ),

	END_LAT FLOAT AS (
        coalesce(CAST(GET_IGNORE_CASE($1, 'END_LAT') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'DROPOFF_LATITUDE') AS FLOAT), 0)
    ),

	PULOCATIONID NUMBER(38,0) AS (CAST(GET_IGNORE_CASE($1, 'PULOCATIONID') AS NUMBER(38,0))),
	DOLOCATIONID NUMBER(38,0) AS (CAST(GET_IGNORE_CASE($1, 'DOLOCATIONID') AS NUMBER(38,0))),
-- fare amount
	FARE_AMOUNT FLOAT AS (
        coalesce(CAST(GET_IGNORE_CASE($1, 'FARE_AMOUNT') AS FLOAT), 0) +
	    coalesce(CAST(GET_IGNORE_CASE($1, 'FARE_AMT') AS FLOAT), 0)
    ),

	EXTRA FLOAT AS (CAST(GET_IGNORE_CASE($1, 'EXTRA') AS FLOAT)),
	MTA_TAX FLOAT AS (CAST(GET_IGNORE_CASE($1, 'MTA_TAX') AS FLOAT)),

	TIP_AMOUNT FLOAT AS (
        coalesce(CAST(GET_IGNORE_CASE($1, 'TIP_AMOUNT') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'TIP_AMT') AS FLOAT), 0)
    ),

	TOLLS_AMOUNT FLOAT AS (
        coalesce(CAST(GET_IGNORE_CASE($1, 'TOLLS_AMOUNT') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'TOLLS_AMT') AS FLOAT), 0)
    ),

	IMPROVEMENT_SURCHARGE FLOAT AS (CAST(GET_IGNORE_CASE($1, 'IMPROVEMENT_SURCHARGE') AS FLOAT)),

	TOTAL_AMOUNT FLOAT AS (
        coalesce(CAST(GET_IGNORE_CASE($1, 'TOTAL_AMOUNT') AS FLOAT), 0) +
        coalesce(CAST(GET_IGNORE_CASE($1, 'TOTAL_AMT') AS FLOAT), 0)
    ),

	CONGESTION_SURCHARGE FLOAT AS (CAST(GET_IGNORE_CASE($1, 'CONGESTION_SURCHARGE') AS FLOAT)),

	AIRPORT_FEE VARIANT AS (CAST(GET_IGNORE_CASE($1, 'AIRPORT_FEE') AS VARIANT)),

-- others
	PAYMENT_TYPE VARIANT AS (CAST(GET_IGNORE_CASE($1, 'PAYMENT_TYPE') AS VARIANT)),

	SURCHARGE FLOAT AS (CAST(GET_IGNORE_CASE($1, 'SURCHARGE') AS FLOAT)),

	RATE_CODE VARIANT AS (CAST(GET_IGNORE_CASE($1, 'RATE_CODE') AS VARIANT)),
	RATECODEID VARIANT AS (CAST(GET_IGNORE_CASE($1, 'RATECODEID') AS VARIANT))
)
PARTITION BY (START_DATE)
location=@S3_STAGE/
auto_refresh=false
file_format=my_parquet_format
;