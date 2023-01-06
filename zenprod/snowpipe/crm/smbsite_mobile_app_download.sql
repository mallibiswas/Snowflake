-------------------------------------------------------------------
----------------- SMBSITE_MOBILE_APP_DOWNLOAD table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_MOBILE_APP_DOWNLOAD_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_MOBILE_APP_DOWNLOAD as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'user_id:$oid')::string as user_id,
      GET_PATH($1, 'banner_dismissed:$date')::timestamp as banner_dismissed,
      $1:engagement_type::string as engagement_type,
      GET_PATH($1, 'banner_first_seen:$date')::timestamp as banner_first_seen,
      $1:view_type::string as view_type,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_modileappdownload.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_MOBILE_APP_DOWNLOAD_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_MOBILE_APP_DOWNLOAD_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_MOBILE_APP_DOWNLOAD_TASK resume;
