-------------------------------------------------------------------
----------------- MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'completed:$date')::timestamp as completed,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:page::string as portal,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_onboardingwizardpagecompletions.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS_TASK resume;
