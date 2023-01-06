-------------------------------------------------------------------
----------------- PORTAL_TO_CONSENT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.PORTAL_TO_CONSENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.PORTAL_TO_CONSENT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      $1:client_mac::string as client_mac,
      GET_PATH($1, 'created:$date')::timestamp as date,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_tosconsent.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.PORTAL_TO_CONSENT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.PORTAL_TO_CONSENT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.PORTAL_TO_CONSENT_TASK resume;
