-------------------------------------------------------------------
----------------- MOBILE_DEVICE_REGISTRATION_LOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.MOBILE_DEVICE_REGISTRATION_LOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.MOBILE_DEVICE_REGISTRATION_LOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:push_id::string as push_id,
      $1:login_email::string as login_email,
      $1:device_id::string as device_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/mobile_deviceregistrationlog.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.MOBILE_DEVICE_REGISTRATION_LOG_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.MOBILE_DEVICE_REGISTRATION_LOG_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.MOBILE_DEVICE_REGISTRATION_LOG_TASK resume;
