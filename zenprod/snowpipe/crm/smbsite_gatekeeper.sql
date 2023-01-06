-------------------------------------------------------------------
----------------- SMBSITE_GATEKEEPER table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_GATEKEEPER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_GATEKEEPER as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:name::string as name,
      $1:description::string as description,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_gatekeeper.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_GATEKEEPER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_GATEKEEPER_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_GATEKEEPER_TASK resume;
