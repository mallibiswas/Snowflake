-------------------------------------------------------------------
----------------- MODELS_ACCOUNT_TUTORIAL_COMPLETION table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      $1:tutorial::string as tutorial,
      GET_PATH($1, 'completed:$date')::timestamp as completed,
      $1:extra_args::variant as extra_args,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_accounttutorialcompletion.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.MODELS_ACCOUNT_TUTORIAL_COMPLETION_TASK resume;
