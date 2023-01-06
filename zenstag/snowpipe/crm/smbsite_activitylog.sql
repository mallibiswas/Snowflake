-------------------------------------------------------------------
----------------- SMBSITE_ACTIVITYLOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.SMBSITE_ACTIVITYLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.SMBSITE_ACTIVITYLOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'user_id:$oid')::string as user_id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      $1:is_staff::boolean as is_staff,
      $1:activity::string as activity,
      GET_PATH($1, 'latest:$date')::timestamp as latest,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_activitylog.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.SMBSITE_ACTIVITYLOG_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.SMBSITE_ACTIVITYLOG_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.SMBSITE_ACTIVITYLOG_TASK resume;
