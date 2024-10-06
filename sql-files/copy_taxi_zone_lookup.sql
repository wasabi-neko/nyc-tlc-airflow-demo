alter session set query_tag='nyc-tlc-demo';
use schema nyc_tlc.public;
use warehouse compute_wh;

create or replace file format csv_header_format
    TYPE = 'CSV'
    field_delimiter = ','
    skip_header = 1;


create or replace table taxi_zone_lookup(
    locationid int,
    borough varchar(256),
    zone varchar(256),
    service_zone varchar(265)
);


create or replace STAGE taxi_zone_stage
    url = 's3://nyc-tlc-demo/taxi_zone_lookup.csv'
    STORAGE_INTEGRATION = s3_integration
    file_format = csv_header_format;


copy into taxi_zone_lookup
from @taxi_zone_stage;

-- select * from taxi_zone_lookup;