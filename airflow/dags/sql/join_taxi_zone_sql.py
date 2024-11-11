# create final table with join yellow table with taxi

sql_str ="""
ALTER SESSION SET QUERY_TAG = {query_tag};
USE SCHEMA {schema};
USE WAREHOUSE {warehouse};


CREATE OR REPLACE TABLE {result_table} AS
SELECT 
    PICKUP_DATETIME,
    DROPOFF_DATETIME,
    D.PASSENGER_COUNT,
    TRIP_DISTANCE,
    COALESCE(D.START_LON, START_LOC.X) AS START_LON,
    COALESCE(D.START_LAT, START_LOC.Y) AS START_LAT,
    COALESCE(D.END_LON, END_LOC.X) AS END_LON,
    COALESCE(D.END_LAT, END_LOC.Y) AS END_LAT

FROM 
    {tripdata_table} AS D
LEFT JOIN
    {taxi_zone_table} AS START_LOC ON D.PULOCATIONID = START_LOC.LOCATIONID
LEFT JOIN
    {taxi_zone_table} AS END_LOC ON D.DOLOCATIONID = END_LOC.LOCATIONID
;
"""


def get_sql_str(result, tripdata, taxi_zone, query_tag, schema, warehouse):
    return sql_str.format(
        query_tag=query_tag,
        schema=schema,
        warehouse=warehouse,
        result_table=result,
        tripdata_table=tripdata,
        taxi_zone_table=taxi_zone
    )