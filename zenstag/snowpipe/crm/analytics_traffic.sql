-------------------------------------------------------------------
----------------- ANALYTICS_TRAFFIC table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.ANALYTICS_TRAFFIC_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.ANALYTICS_TRAFFIC as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:avg_visit_duration::integer as avg_visit_duration,
      GET_PATH($1, 'updated:$date')::datetime as updated,
      $1:repeat_visitors::integer as repeat_visitors,
      GET_PATH($1, 'timestamp:$date')::datetime as timestamp,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:period::integer as period,
      $1:visitors::integer as visitors,
      $1:passersby::integer as passersby,
      $1:new_visitors::integer as new_visitors,
      $1:converted_visitors::integer as converted_visitors,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_traffic.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.ANALYTICS_TRAFFIC_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.ANALYTICS_TRAFFIC_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.ANALYTICS_TRAFFIC_TASK resume;
