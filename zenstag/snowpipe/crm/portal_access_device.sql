-------------------------------------------------------------------
----------------- PORTAL_ACCESS_DEVICE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.PORTAL_ACCESS_DEVICE as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:mac::string as mac,
      GET_PATH($1, 'last_seen:$date')::timestamp as last_seen,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_accessdevice.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_TASK resume;
