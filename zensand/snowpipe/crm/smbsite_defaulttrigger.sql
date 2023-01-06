-------------------------------------------------------------------
----------------- SMBSITE_DEFAULTTRIGGER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_DEFAULTTRIGGER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_DEFAULTTRIGGER as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'proximity_rule:$oid')::string as proximity_rule,
      $1:is_recurring::boolean as is_recurring,
      $1:description::string as description,
      $1:parameters::variant as parameters,
      $1:title::string as title,
      $1:demographic_rule::string as demographic_rule,
      $1:purchase_rule::variant as purchase_rule,
      $1:enabled::boolean as enabled,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:rule::variant as rule,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_defaulttrigger.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_DEFAULTTRIGGER_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_DEFAULTTRIGGER_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_DEFAULTTRIGGER_TASK resume;
