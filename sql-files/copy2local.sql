alter session set query_tag='nyc-tlc-demo-extable';


use warehouse compute_large;
CREATE or replace TABLE yellow_final AS
SELECT
    START_DATE,
    VENDOR_ID, vendor_name
    tpep_pickup_datetime, tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    start_lon, start_lat, end_lon, end_lat,
    PULOCATIONID, DOLOCATIONID,
    EXTRA, TIP_AMOUNT, AIRPORT_FEE
from yellow_super_extable;
