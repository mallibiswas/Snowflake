-------------------------------------------------------------------
----------------- SMBSITE_MERCHANTNOTIFICATION_SETTING table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_MERCHANTNOTIFICATION_SETTING_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_MERCHANTNOTIFICATION_SETTING as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      $1:email::string as email,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      GET_PATH($1, 'updated:$date')::timestamp as updated,
      GET_PATH($1, 'reputation_notification:email_enabled')::boolean as email_enabled,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_merchantnotificationsetting.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_MERCHANTNOTIFICATION_SETTING_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_MERCHANTNOTIFICATION_SETTING_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_MERCHANTNOTIFICATION_SETTING_TASK resume;
