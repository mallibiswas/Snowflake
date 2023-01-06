-------------------------------------------------------------------
----------------- SMBSITE_AUTHLOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_AUTHLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_AUTHLOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:username::string as username,
      $1:account_known::boolean as account_known,
      $1:login_success::boolean as login_success,
      $1:failed_attempts::integer as failed_attempts,
      GET_PATH($1, 'account_lock:$date')::timestamp as account_lock,
      GET_PATH($1, 'last_successful_login:$date')::timestamp as last_sucessful_login,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_authlog.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.SMBSITE_AUTHLOG_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_AUTHLOG_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_AUTHLOG_TASK resume;
