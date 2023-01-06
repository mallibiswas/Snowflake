-------------------------------------------------------------------
----------------- SMBSITE_TRIGGER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_TRIGGER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_TRIGGER as
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
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_trigger.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_TRIGGER_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_TRIGGER_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_TRIGGER_TASK resume;
