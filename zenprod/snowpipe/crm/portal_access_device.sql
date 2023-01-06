-------------------------------------------------------------------
----------------- PORTAL_ACCESS_DEVICE table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.PORTAL_ACCESS_DEVICE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.PORTAL_ACCESS_DEVICE as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:mac::string as mac,
      GET_PATH($1, 'last_seen:$date')::timestamp as last_seen,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_accessdevice.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.PORTAL_ACCESS_DEVICE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.PORTAL_ACCESS_DEVICE_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.PORTAL_ACCESS_DEVICE_TASK resume;
