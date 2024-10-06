# NYC-TLC DEMO

using airflow and snowflake

## NYC-TLC Data

[nyc.gov tlc trip data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

### Data Format

As yellow_tripdata as examples, the column naming had been changed through several times.
Like: `VENDOR_ID` `VENDORNAME` `VENDORID`.

All the data start using location id insteand actual lat and lon around 2015.

## S3 bucket structure

I fetch the files from [offical website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) via my script 
`crawler/prepare_data.python`

```
nyc-tlc-demo
├── taxi_zone_lookup.csv
└── trip-data
    ├── yellow
    │   ├── yellow_tripdata_2009-01.parquet
    │   └── ...
    ├── green
    │   ├── green_tripdata_2014-01.parquet
    │   └── ...
    ├── fhv
    │   ├── fhv_tripdata_2015-01.parquet
    │   └── ...
    └── fhvhv
        ├── fhvhv_tripdata_2019-02.parquet
        └── ...
```

## Workflow

Method1: Copy data from external table

```
copy_from_s3: from external table to table
transformation 1: unify the column naming
transformation 2: join taxi_zone_lookup with locationID
merge_all_table: merge all from yellow, green, fhv
```

Method2: Copy file to internal stage first, then create table from internal stage

```
copy file to internal stage from external stage
create table from internal stage
transformation 1: unify the column naming
transformation 2: join taxi_zone_lookup with locationID
merge_all_table: merge all from yellow, green, fhv
remove file in local stage
```