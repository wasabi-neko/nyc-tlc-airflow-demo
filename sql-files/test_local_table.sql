alter session set query_tag='nyc-tlc-demo-extable';
desc table final_table;
select start_date, sum(passenger_count) from final_table
group by start_date
order by start_date;

alter session set use_cached_result = false;
select year(start_date), sum(passenger_count) from final_table
group by year(start_date) 
order by year(start_date);