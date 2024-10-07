alter session set query_tag = "{'project': 'nyc-tlc-demo', 'method': 'copy_from_external_stage'}";
use schema nyc_tlc.public;
use warehouse compute_wh;

create or replace file format csv_header_format
    TYPE = 'CSV'
    field_delimiter = ','
    skip_header = 1;

create or replace table taxi_zone(
    x float,
    y float,
    zone varchar(256),
    locationid int,
    borough varchar(256)
);

create or replace stage taxi_zone_stage
    url = 's3://nyc-tlc-demo/taxi_zones.csv'
    STORAGE_INTEGRATION = s3_integration
    file_format = csv_header_format;

copy into taxi_zone 
from @taxi_zone_stage;

drop stage taxi_zone_stage;