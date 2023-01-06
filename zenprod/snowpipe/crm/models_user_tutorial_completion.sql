-------------------------------------------------------------------
----------------- MODELS_USER_TUTORIAL_COPMLETION table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.MODELS_USER_TUTORIAL_COPMLETION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.MODELS_USER_TUTORIAL_COPMLETION as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:skipped_all::boolean as skipped_all,
      $1:completed::variant as completed,
      $1:email::string as email,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_usertutorialcompletion.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.MODELS_USER_TUTORIAL_COPMLETION_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.MODELS_USER_TUTORIAL_COPMLETION_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.MODELS_USER_TUTORIAL_COPMLETION_TASK resume;

