drop table if exists adhoc.cloud_run_test;
create table if not exists adhoc.cloud_run_test as 
select current_datetime() as now
;