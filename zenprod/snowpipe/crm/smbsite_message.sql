-------------------------------------------------------------------
----------------- SMBSITE_MESSAGE table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_MESSAGE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_MESSAGE as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:archived::boolean as archived,
      $1:is_referenced::boolean as is_referenced,
      GET_PATH($1, 'created:$date')::timestamp as created,
      $1:daily_limit::integer as daily_limit,
      GET_PATH($1, 'template_id:$oid')::string as template_id,
      $1:subject::string as subject,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_message.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_MESSAGE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_MESSAGE_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_MESSAGE_TASK resume;
