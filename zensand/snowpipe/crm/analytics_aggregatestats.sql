-------------------------------------------------------------------
----------------- ANALYTICS_AGGREGATESTATS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.ANALYTICS_AGGREGATESTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.ANALYTICS_AGGREGATESTATS as
    select  
      GET_PATH($1, '_id:$oid') as id,
      GET_PATH($1, 'business_id:$oid') as business_id,
      GET_PATH($1, 'updated:$date')::datetime as updated,
      GET_PATH($1, 'created:$date')::datetime as created,
      $1:reach::integer as reach,
      $1:period::integer as period,
      current_timestamp() as asof_date
       FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_aggregatestats.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.ANALYTICS_AGGREGATESTATS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.ANALYTICS_AGGREGATESTATS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.ANALYTICS_AGGREGATESTATS_TASK resume;
