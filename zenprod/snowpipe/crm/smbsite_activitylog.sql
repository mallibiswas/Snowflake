-------------------------------------------------------------------
----------------- SMBSITE_ACTIVITYLOG table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_ACTIVITYLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_ACTIVITYLOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'user_id:$oid')::string as user_id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      $1:is_staff::boolean as is_staff,
      $1:activity::string as activity,
      GET_PATH($1, 'latest:$date')::timestamp as latest,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_activitylog.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_ACTIVITYLOG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_ACTIVITYLOG_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_ACTIVITYLOG_TASK resume;
