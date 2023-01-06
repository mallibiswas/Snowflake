-------------------------------------------------------------------
----------------- MODELS_USER_TUTORIAL_COPMLETION table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.MODELS_USER_TUTORIAL_COPMLETION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.MODELS_USER_TUTORIAL_COPMLETION as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:skipped_all::boolean as skipped_all,
      $1:completed::variant as completed,
      $1:email::string as email,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/models_usertutorialcompletion.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.MODELS_USER_TUTORIAL_COPMLETION_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.MODELS_USER_TUTORIAL_COPMLETION_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.MODELS_USER_TUTORIAL_COPMLETION_TASK resume;

