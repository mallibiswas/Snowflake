-------------------------------------------------------------------
----------------- MOBILE_NOTIFICATION_DEVICE_LOGIN table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'last_login:$date')::timestamp as last_login,
      $1:device_id::string as device_id,
      GET_PATH($1, 'archived:$date')::timestamp as archived,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/mobile_notificationdevicelogin.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_LOGIN_TASK resume;
