-------------------------------------------------------------------
----------------- SMBSITE_EMAILTEMPLATE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.SMBSITE_EMAILTEMPLATE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.SMBSITE_EMAILTEMPLATE as
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
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_emailtemplate.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSTAG.CRM.SMBSITE_EMAILTEMPLATE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.SMBSITE_EMAILTEMPLATE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.SMBSITE_EMAILTEMPLATE_TASK resume;
