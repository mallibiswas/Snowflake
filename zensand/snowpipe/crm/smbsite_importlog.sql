-------------------------------------------------------------------
----------------- SMBSITE_IMPORTLOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_IMPORTLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_IMPORTLOG as
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
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_importlog.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_IMPORTLOG_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_IMPORTLOG_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_IMPORTLOG_TASK resume;
