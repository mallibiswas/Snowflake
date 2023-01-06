-------------------------------------------------------------------
----------------- SMBSITE_MESSAGE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_MESSAGE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_MESSAGE as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:archived::boolean as archived,
      $1:is_referenced::boolean as is_referenced,
      GET_PATH($1, 'created:$date')::timestamp as created,
      $1:daily_limit::integer as daily_limit,
      GET_PATH($1, 'template_id:$oid')::string as template_id,
      $1:subject::string as subject,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_message.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_MESSAGE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_MESSAGE_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_MESSAGE_TASK resume;
