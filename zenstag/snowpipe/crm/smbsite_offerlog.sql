-------------------------------------------------------------------
----------------- SMBSITE_OFFERLOG table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.SMBSITE_OFFERLOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.SMBSITE_OFFERLOG as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:code::string as code,
      GET_PATH($1, 'opened:$date')::timestamp as opened,
      GET_PATH($1, 'created:$date')::timestamp as created,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'redeemed:$date')::timestamp as redeemed,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'offer_id:$oid')::string as offer_id,
      $1:test_user::boolean as test_user,
      GET_PATH($1, 'expiration:$date')::timestamp as expiration,
      $1:redirect_id::string as redirect_id,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_offerlog.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSTAG.CRM.SMBSITE_OFFERLOG_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.SMBSITE_OFFERLOG_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.SMBSITE_OFFERLOG_TASK resume;
