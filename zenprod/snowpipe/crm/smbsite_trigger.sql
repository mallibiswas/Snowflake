-------------------------------------------------------------------
----------------- SMBSITE_TRIGGER table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_TRIGGER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_TRIGGER as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:is_recurring::boolean as is_recurring,
      $1:parameters::variant as parameters,
      $1:title::string as title,
      $1:purchase_rule::string as purchase_rule,
      $1:enabled::boolean as enabled,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:rule::variant as rule,
      GET_PATH($1, 'parent_id:$oid')::string as parent_id,
      GET_PATH($1, 'message_id:$oid')::string as message_id,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_trigger.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_TRIGGER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_TRIGGER_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_TRIGGER_TASK resume;
