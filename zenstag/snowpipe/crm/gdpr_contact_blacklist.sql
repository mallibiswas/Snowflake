-------------------------------------------------------------------
----------------- GDPR_CONTACT_BLACKLIST table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.GDPR_CONTACT_BLACKLIST_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.GDPR_CONTACT_BLACKLIST as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'contact_id:$oid')::string as contact_id,
      GET_PATH($1, 'account_id:$oid')::string as account_id,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/gdpr_contact_blacklist.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.GDPR_CONTACT_BLACKLIST_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
    CALL ZENSTAG.CRM.GDPR_CONTACT_BLACKLIST_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.GDPR_CONTACT_BLACKLIST_TASK resume;
