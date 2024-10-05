desc table super_extable;

use warehouse compute_large;
CREATE TABLE final_table AS
SELECT
    START_DATE,
    VENDOR_ID, vendor_name
    tpep_pickup_datetime, tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    start_lon, start_lat, end_lon, end_lat,
    PULOCATIONID, DOLOCATIONID,
    EXTRA, TIP_AMOUNT, AIRPORT_FEE
from super_extable;
