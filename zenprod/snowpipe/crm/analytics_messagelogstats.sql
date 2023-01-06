-------------------------------------------------------------------
----------------- ANALYTICS_MESSAGELOGSTATS table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.ANALYTICS_MESSAGELOGSTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.ANALYTICS_MESSAGELOGSTATS as
    select  
      GET_PATH($1, '_id:$oid') as id,
      GET_PATH($1, 'timestamp:$date')::datetime as timestamp,
      $1:period::integer as period,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_messagelogstats.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.ANALYTICS_MESSAGELOGSTATS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.ANALYTICS_MESSAGELOGSTATS_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.ANALYTICS_MESSAGELOGSTATS_TASK resume;
