-------------------------------------------------------------------
----------------- ANALYTICS_AGGREGATESTATS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.ANALYTICS_AGGREGATESTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.ANALYTICS_AGGREGATESTATS as
    select  
      GET_PATH($1, '_id:$oid') as id,
      GET_PATH($1, 'business_id:$oid') as business_id,
      GET_PATH($1, 'updated:$date')::datetime as updated,
      GET_PATH($1, 'created:$date')::datetime as created,
      $1:reach::integer as reach,
      $1:period::integer as period,
      current_timestamp() as asof_date
       FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_aggregatestats.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.ANALYTICS_AGGREGATESTATS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.ANALYTICS_AGGREGATESTATS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.ANALYTICS_AGGREGATESTATS_TASK resume;
