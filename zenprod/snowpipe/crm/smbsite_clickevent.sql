-------------------------------------------------------------------
----------------- SMBSITE_CLICKEVENT table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_CLICKEVENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_CLICKEVENT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'click_time:$date')::timestamp as click_time,
      $1:short_url::string as short_url,
      $1:long_url::string as long_url,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_clickevent.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_CLICKEVENT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_CLICKEVENT_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_CLICKEVENT_TASK resume;
