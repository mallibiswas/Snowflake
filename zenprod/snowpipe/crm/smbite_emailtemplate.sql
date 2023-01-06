-------------------------------------------------------------------
----------------- SMBSITE_EMAILTEMPLATE table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_EMAILTEMPLATE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_EMAILTEMPLATE as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'updated:$date')::timestamp as updated,
      $1:layout::variant as layout,
      $1:complete::boolean as complete,
      GET_PATH($1, 'created:$date')::timestamp as created,
      $1:deleted::boolean as deleted,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:preview_text::string as preview_text,
      $1:subject::string as subject,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_emailtemplate.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_EMAILTEMPLATE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_EMAILTEMPLATE_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_EMAILTEMPLATE_TASK resume;
