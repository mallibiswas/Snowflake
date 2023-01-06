-------------------------------------------------------------------
----------------- MOBILE_DEVICE_REGISTRATION_LOG table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.MOBILE_DEVICE_REGISTRATION_LOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.MOBILE_DEVICE_REGISTRATION_LOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:push_id::string as push_id,
      $1:login_email::string as login_email,
      $1:device_id::string as device_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/mobile_deviceregistrationlog.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.MOBILE_DEVICE_REGISTRATION_LOG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.MOBILE_DEVICE_REGISTRATION_LOG_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.MOBILE_DEVICE_REGISTRATION_LOG_TASK resume;
