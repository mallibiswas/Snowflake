-------------------------------------------------------------------
----------------- SMBSITE_CLICKLOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_CLICKLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_CLICKLOG as
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
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_clicklog.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.SMBSITE_CLICKLOG_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_CLICKLOG_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_CLICKLOG_TASK resume;
