-------------------------------------------------------------------
----------------- MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'completed:$date')::timestamp as completed,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:page::string as portal,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_onboardingwizardpagecompletions.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.MODELS_ONBOARDING_WIZARD_PAGE_COMPLETIONS_TASK resume;
