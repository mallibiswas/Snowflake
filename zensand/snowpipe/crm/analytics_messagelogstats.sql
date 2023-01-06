-------------------------------------------------------------------
----------------- ANALYTICS_MESSAGELOGSTATS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.ANALYTICS_MESSAGELOGSTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.ANALYTICS_MESSAGELOGSTATS as
    select  
      GET_PATH($1, '_id:$oid') as id,
      GET_PATH($1, 'timestamp:$date')::datetime as timestamp,
      $1:period::integer as period,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_messagelogstats.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.ANALYTICS_MESSAGELOGSTATS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.ANALYTICS_MESSAGELOGSTATS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.ANALYTICS_MESSAGELOGSTATS_TASK resume;
