-------------------------------------------------------------------
----------------- PORTAL_USERPROFILE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.PORTAL_USERPROFILE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.PORTAL_USERPROFILE as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:information_source::string as information_source,
      GET_PATH($1, 'user_id:$oid')::string as user_id,
      $1:credits::integer as credits,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      $1:email::string as email,
      $1:email_is_valid::boolean as email_is_valid,
      GET_PATH($1, 'email_last_validated:$date')::timestamp as email_last_validated,
      $1:email_reason::string as email_reason,
      $1:email_score::float as email_score,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_userprofile.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.PORTAL_USERPROFILE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.PORTAL_USERPROFILE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.PORTAL_USERPROFILE_TASK resume;
