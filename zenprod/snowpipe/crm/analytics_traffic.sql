-------------------------------------------------------------------
----------------- ANALYTICS_TRAFFIC table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.ANALYTICS_TRAFFIC_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.ANALYTICS_TRAFFIC as
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
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_traffic.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.ANALYTICS_TRAFFIC_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.ANALYTICS_TRAFFIC_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.ANALYTICS_TRAFFIC_TASK resume;
