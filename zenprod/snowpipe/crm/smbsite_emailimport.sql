-------------------------------------------------------------------
----------------- SMBSITE_EMAILIMPORT table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_EMAILIMPORT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_EMAILIMPORT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:mac_count::integer as mac_count,
      $1:duplicate_count::integer as duplicate_count,
      GET_PATH($1, 'created:$date')::timestamp as created,
      $1:tags::variant as tags,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:filename::string as file_name,
      GET_PATH($1, 'uploaded_by_id:$oid')::string as uploaded_by_id,
      $1:customers_count::integer as customers_count,
      $1:file::string as file,
      $1:ignored_count::integer as ignored_count,
      $1:lines_count::integer as lines_count,
      $1:error::string as error,
      GET_PATH($1, 'undone:$date')::timestamp as undone,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_emailimport.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.SMBSITE_EMAILIMPORT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_EMAILIMPORT_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_EMAILIMPORT_TASK resume;
