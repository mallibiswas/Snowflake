-------------------------------------------------------------------
----------------- MODELS_BILLING_AND_SUBSCRIPTION_PREFS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:enrichment_enabled::boolean as enrichment_enabled,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_billingandsubscriptionprefs.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS_TASK resume;
