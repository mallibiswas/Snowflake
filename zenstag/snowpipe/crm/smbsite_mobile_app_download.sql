-------------------------------------------------------------------
----------------- SMBSITE_MOBILE_APP_DOWNLOAD table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.SMBSITE_MOBILE_APP_DOWNLOAD_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.SMBSITE_MOBILE_APP_DOWNLOAD as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'user_id:$oid')::string as user_id,
      GET_PATH($1, 'banner_dismissed:$date')::timestamp as banner_dismissed,
      $1:engagement_type::string as engagement_type,
      GET_PATH($1, 'banner_first_seen:$date')::timestamp as banner_first_seen,
      $1:view_type::string as view_type,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_modileappdownload.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSTAG.CRM.SMBSITE_MOBILE_APP_DOWNLOAD_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.SMBSITE_MOBILE_APP_DOWNLOAD_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.SMBSITE_MOBILE_APP_DOWNLOAD_TASK resume;
