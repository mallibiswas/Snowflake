-------------------------------------------------------------------
----------------- SMBSITE_IMPORTLOG table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_IMPORTLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_IMPORTLOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:username::string as username,
      $1:success::boolean as success,
      $1:tags::variant as tags,
      GET_PATH($1, 'started:$date')::timestamp as started,
      GET_PATH($1, 'completed:$date')::timestamp as completed,
      $1:webhook::boolean as webhook,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'emailimport_id:$oid')::string as emailimport_id,
      $1:source::string as source,
      GET_PATH($1, 'imported_by_id:$oid')::string as imported_by_id,
      $1:contacts_found::integer as contact_found,
      $1:contacts_added:integer as contacts_added,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_importlog.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.SMBSITE_IMPORTLOG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_IMPORTLOG_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_IMPORTLOG_TASK resume;
