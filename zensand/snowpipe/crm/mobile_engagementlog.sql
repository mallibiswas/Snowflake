-------------------------------------------------------------------
----------------- MOBILE_ENGAGEMENT_LOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.MOBILE_ENGAGEMENT_LOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.MOBILE_ENGAGEMENT_LOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'sent:$date')::timestamp as sent,
      $1:device_id::string as device_id,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/mobile_engagementlog.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.MOBILE_ENGAGEMENT_LOG_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.MOBILE_ENGAGEMENT_LOG_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.MOBILE_ENGAGEMENT_LOG_TASK resume;
