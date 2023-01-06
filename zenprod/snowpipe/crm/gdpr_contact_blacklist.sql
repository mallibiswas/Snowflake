-------------------------------------------------------------------
----------------- GDPR_CONTACT_BLACKLIST table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.GDPR_CONTACT_BLACKLIST_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.GDPR_CONTACT_BLACKLIST as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'contact_id:$oid')::string as contact_id,
      GET_PATH($1, 'account_id:$oid')::string as account_id,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/gdpr_contact_blacklist.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.GDPR_CONTACT_BLACKLIST_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
    CALL ZENPROD.CRM.GDPR_CONTACT_BLACKLIST_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.GDPR_CONTACT_BLACKLIST_TASK resume;
