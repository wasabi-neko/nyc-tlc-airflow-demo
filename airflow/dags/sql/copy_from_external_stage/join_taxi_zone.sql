alter session set query_tag = "{'project': 'nyc-tlc-demo', 'method': 'copy_from_external_stage'}";
use schema nyc_tlc.public;
use warehouse compute_large;


create or replace table yellow_final as
select 
    pickup_datetime,
    dropoff_datetime,
    d.passenger_count,
    trip_distance,
    coalesce(d.start_lat ,start_loc.x) as start_lat,
    coalesce(d.start_lon ,start_loc.y) as start_lon,
    coalesce(d.end_lat ,end_loc.x) as end_lat,
    coalesce(d.end_lon ,end_loc.y) as end_lon

from 
    yellow_trip_data as d
INNER JOIN
    taxi_zone as start_loc on d.pulocationid = start_loc.locationid
INNER join
    taxi_zone as end_loc on d.dolocationid = end_loc.locationid
;