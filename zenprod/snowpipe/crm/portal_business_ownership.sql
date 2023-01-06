-------------------------------------------------------------------
----------------- PORTAL_BUSINESS_OWNERSHIP table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.PORTAL_BUSINESS_OWNERSHIP_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.PORTAL_BUSINESS_OWNERSHIP as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'updated:$date')::timestamp as updated,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'role_ids[0]')::string as role_id,
      GET_PATH($1, 'create:$date')::timestamp as created,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_businessownership.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.PORTAL_BUSINESS_OWNERSHIP_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.PORTAL_BUSINESS_OWNERSHIP_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.PORTAL_BUSINESS_OWNERSHIP_TASK resume;


