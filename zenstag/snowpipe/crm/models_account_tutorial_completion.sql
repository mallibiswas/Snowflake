-------------------------------------------------------------------
----------------- MODELS_ACCOUNT_TUTORIAL_COMPLETION table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      $1:tutorial::string as tutorial,
      GET_PATH($1, 'completed:$date')::timestamp as completed,
      $1:extra_args::variant as extra_args,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_accounttutorialcompletion.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION_TASK resume;
