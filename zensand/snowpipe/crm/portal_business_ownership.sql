-------------------------------------------------------------------
----------------- PORTAL_BUSINESS_OWNERSHIP table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.PORTAL_BUSINESS_OWNERSHIP_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.PORTAL_BUSINESS_OWNERSHIP as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'updated:$date')::timestamp as updated,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'role_ids[0]')::string as role_id,
      GET_PATH($1, 'create:$date')::timestamp as created,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_businessownership.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.PORTAL_BUSINESS_OWNERSHIP_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.PORTAL_BUSINESS_OWNERSHIP_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.PORTAL_BUSINESS_OWNERSHIP_TASK resume;


