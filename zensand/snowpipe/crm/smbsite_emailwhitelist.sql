-------------------------------------------------------------------
----------------- SMBSITE_EMAILWHITELIST table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_EMAILWHITELIST_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_EMAILWHITELIST as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:email::string as email,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_emailwhitelist.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_EMAILWHITELIST_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_EMAILWHITELIST_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_EMAILWHITELIST_TASK resume;
