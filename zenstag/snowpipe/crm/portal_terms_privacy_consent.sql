-------------------------------------------------------------------
----------------- PORTAL_TERM_PRIVACY_CONSENT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.PORTAL_TERM_PRIVACY_CONSENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.PORTAL_TERM_PRIVACY_CONSENT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      GET_PATH($1, 'consent_time:$date')::timestamp as consent_time,
      $1:userprofile_email::string as userprofile_email,
      $1:client_mac::string as client_mac,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:bundle_version::string as bundle_version,
      $1:terms_version::string as terms_version,
      $1:privacy_version::string as privacy_version,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_portaltermsprivacyconsent.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.PORTAL_TERM_PRIVACY_CONSENT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.PORTAL_TERM_PRIVACY_CONSENT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.PORTAL_TERM_PRIVACY_CONSENT_TASK resume;
