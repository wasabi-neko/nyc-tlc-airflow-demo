use warehouse compute_wh;

desc table snowflake.account_usage.query_history;

-- List the information about the nyc-tlc-load queries
select 
    query_id, 
    query_text,
    try_parse_json(query_tag)['type'] as type,
    CAST(try_parse_json(query_tag)['dag_timestamp'] AS datetime)as dag_timestamp
from snowflake.account_usage.query_history
where
    try_parse_json(query_tag)['project']::string = 'nyc-tlc-load-v2' 
order by 
    try_parse_json(query_tag)['dag_timestamp'] desc
;


-- List the Executing time of nyc-tlc-load dag-run
select
    count(QUERY_ID),
    try_parse_json(query_tag)['type']::string as query_type,
    timediff(second, min(start_time), MAX(end_time)) as real_duration_sec,
    CAST(try_parse_json(query_tag)['dag_timestamp'] AS datetime)as dag_timestamp,
    MIN(start_time),
    MAX(end_time),
from
    snowflake.account_usage.query_history
where 
    try_parse_json(query_tag)['project']::string = 'nyc-tlc-load-v2' and
    execution_status = 'SUCCESS'
GROUP BY
    try_parse_json(query_tag)['dag_timestamp'],
    try_parse_json(query_tag)['type']::string
ORDER BY
    dag_timestamp desc;

-- ----------------------------------------
-- V2 query
-- ----------------------------------------

-- Compare the Executing time of different type of loading method
select
    count(QUERY_ID),
    try_parse_json(query_tag)['type']::string as query_type,
    -- SUM(total_elapsed_time) as sum_total_elaspe_time_ms,
    timediff(second, min(start_time), MAX(end_time)) as real_duration_sec,
    CAST(try_parse_json(query_tag)['dag_timestamp'] AS datetime)as dag_timestamp,
    MIN(start_time),
    MAX(end_time),
from
    snowflake.account_usage.query_history
where 
    try_parse_json(query_tag)['project']::string = 'nyc-tlc-demo-compare-v2' and
    execution_status = 'SUCCESS'

GROUP BY
    try_parse_json(query_tag)['dag_timestamp'],
    try_parse_json(query_tag)['type']::string
ORDER BY
    dag_timestamp desc;


-- List some information about the nyc-tlc-demo-compare-v2 querys
select 
    query_id, 
    query_text,
    try_parse_json(query_tag)['dag_timestamp'],
    try_parse_json(query_tag)['type'],
    CAST(try_parse_json(query_tag)['dag_timestamp'] AS datetime)as dag_timestamp
from snowflake.account_usage.query_history
where
    try_parse_json(query_tag)['project']::string = 'nyc-tlc-demo-compare-v2' 
order by 
    try_parse_json(query_tag)['dag_timestamp'] desc
;

-- Group by dag_timestemp
select
    count(QUERY_ID),
    listagg(query_text, ';'),
    SUM(total_elapsed_time) as sum_time,
    timediff(second, min(start_time), MAX(end_time)) as real_duration,
    MIN(start_time),
    MAX(end_time),
    CAST(try_parse_json(query_tag)['dag_timestamp'] AS datetime)as dag_timestamp
from snowflake.account_usage.query_history
where 
    try_parse_json(query_tag)['project']::string = 'nyc-tlc-demo-compare-v2' and
    try_parse_json(query_tag)['type']::string = 'external_stage' and
    execution_status = 'SUCCESS'

GROUP BY try_parse_json(query_tag)['dag_timestamp']
ORDER BY real_duration desc;


-- Check warehouse usage during nyc-tlc-demo-compare-v2 query
select query_id, query_text, warehouse_name, query_load_percent
from snowflake.account_usage.query_history
where try_parse_json(query_tag)['project']::string = 'nyc-tlc-demo-compare-v2'
order by try_parse_json(query_tag)['dag_timestamp'] desc;

