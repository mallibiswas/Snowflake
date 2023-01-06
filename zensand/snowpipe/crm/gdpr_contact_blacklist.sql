-------------------------------------------------------------------
----------------- GDPR_CONTACT_BLACKLIST table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.GDPR_CONTACT_BLACKLIST_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.GDPR_CONTACT_BLACKLIST as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'contact_id:$oid')::string as contact_id,
      GET_PATH($1, 'account_id:$oid')::string as account_id,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/gdpr_contact_blacklist.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.GDPR_CONTACT_BLACKLIST_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
    CALL ZENSAND.CRM.GDPR_CONTACT_BLACKLIST_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.GDPR_CONTACT_BLACKLIST_TASK resume;
