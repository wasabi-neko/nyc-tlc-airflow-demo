alter session set query_tag='nyc-tlc-demo';
use schema nyc_tlc.public;
use warehouse compute_large;
create or replace stage yellow_internal_stage;

COPY FILES
INTO @yellow_internal_stage
FROM @s3_stage/trip-data/yellow/;