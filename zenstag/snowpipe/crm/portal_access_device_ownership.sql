-------------------------------------------------------------------
----------------- PORTAL_ACCESS_DEVICE_OWNERSHIP table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_OWNERSHIP_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_OWNERSHIP as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'accessdevice_id:$oid')::string as accessdevice_id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'last_confirmed:$date')::timestamp as last_confirmed,
      GET_PATH($1, 'created:$date')::timestamp as created,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_accessdeviceownership.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_OWNERSHIP_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_OWNERSHIP_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.PORTAL_ACCESS_DEVICE_OWNERSHIP_TASK resume;
