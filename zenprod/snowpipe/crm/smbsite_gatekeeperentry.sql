-------------------------------------------------------------------
----------------- SMBSITE_GATEKEEPER_ENTRY table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_GATEKEEPER_ENTRY_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_GATEKEEPER_ENTRY as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'gatekeeper_id:$oid')::string as gatekeeper_id,
      GET_PATH($1, 'removed:$date')::timestamp as removed,
      GET_PATH($1, 'created:$date')::timestamp as created,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_gatekeeperentry.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_GATEKEEPER_ENTRY_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_GATEKEEPER_ENTRY_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_GATEKEEPER_ENTRY_TASK resume;
