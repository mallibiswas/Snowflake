-------------------------------------------------------------------
----------------- SMBSITE_AUTHLOG table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_AUTHLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_AUTHLOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:username::string as username,
      $1:account_known::boolean as account_known,
      $1:login_success::boolean as login_success,
      $1:failed_attempts::integer as failed_attempts,
      GET_PATH($1, 'account_lock:$date')::timestamp as account_lock,
      GET_PATH($1, 'last_successful_login:$date')::timestamp as last_sucessful_login,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_authlog.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_AUTHLOG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_AUTHLOG_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_AUTHLOG_TASK resume;
