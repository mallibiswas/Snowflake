-------------------------------------------------------------------
----------------- SMBSITE_CLICKLOG table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_CLICKLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_CLICKLOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:link_class::string as link_class,
      $1:url::string as url,
      GET_PATH($1, 'timestamp:$date')::timestamp as timestamp,
      $1:client_os::string as client_os,
      $1:client_type::string as client_type,
      $1:user_agent::string as user_agent,
      $1:device_type::string as device_type,
      GET_PATH($1, 'messagelog_id:$oid')::string as messagelog_id,
      GET_PATH($1, 'message_id:$oid')::string as message_id,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_clicklog.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_CLICKLOG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_CLICKLOG_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_CLICKLOG_TASK resume;
