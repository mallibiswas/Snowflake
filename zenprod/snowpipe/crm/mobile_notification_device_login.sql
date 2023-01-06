-------------------------------------------------------------------
----------------- MOBILE_NOTIFICATION_DEVICE_LOGIN table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'last_login:$date')::timestamp as last_login,
      $1:device_id::string as device_id,
      GET_PATH($1, 'archived:$date')::timestamp as archived,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/mobile_notificationdevicelogin.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN_TASK resume;
