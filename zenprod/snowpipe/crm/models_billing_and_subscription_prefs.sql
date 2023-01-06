-------------------------------------------------------------------
----------------- MODELS_BILLING_AND_SUBSCRIPTION_PREFS table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:enrichment_enabled::boolean as enrichment_enabled,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_billingandsubscriptionprefs.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.MODELS_BILLING_AND_SUBSCRIPTION_PREFS_TASK resume;
