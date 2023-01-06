-------------------------------------------------------------------
----------------- SMBSITE_CLICKEVENT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.SMBSITE_CLICKEVENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.SMBSITE_CLICKEVENT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'click_time:$date')::timestamp as click_time,
      $1:short_url::string as short_url,
      $1:long_url::string as long_url,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_clickevent.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.SMBSITE_CLICKEVENT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.SMBSITE_CLICKEVENT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.SMBSITE_CLICKEVENT_TASK resume;
