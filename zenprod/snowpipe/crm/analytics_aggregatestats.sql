-------------------------------------------------------------------
----------------- ANALYTICS_AGGREGATESTATS table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.ANALYTICS_AGGREGATESTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.ANALYTICS_AGGREGATESTATS as
    select  
      GET_PATH($1, '_id:$oid') as id,
      GET_PATH($1, 'business_id:$oid') as business_id,
      GET_PATH($1, 'updated:$date')::datetime as updated,
      GET_PATH($1, 'created:$date')::datetime as created,
      $1:reach::integer as reach,
      $1:period::integer as period,
      DATEADD(day, -1, CURRENT_DATE()) as asof_date
       FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_aggregatestats.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.ANALYTICS_AGGREGATESTATS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.ANALYTICS_AGGREGATESTATS_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.ANALYTICS_AGGREGATESTATS_TASK resume;
