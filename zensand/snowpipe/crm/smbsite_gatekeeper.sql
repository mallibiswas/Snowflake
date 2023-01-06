-------------------------------------------------------------------
----------------- SMBSITE_GATEKEEPER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_GATEKEEPER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_GATEKEEPER as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:name::string as name,
      $1:description::string as description,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_gatekeeper.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_GATEKEEPER_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_GATEKEEPER_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_GATEKEEPER_TASK resume;
