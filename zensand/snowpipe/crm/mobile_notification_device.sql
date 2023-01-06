-------------------------------------------------------------------
----------------- MOBILE_NOTIFICATION_DEVICE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'last_updated:$date')::timestamp as last_updated,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:device_id::string as device_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/mobile_notificationdevice.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as
    CALL ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.MOBILE_NOTIFICATION_DEVICE_TASK resume;
