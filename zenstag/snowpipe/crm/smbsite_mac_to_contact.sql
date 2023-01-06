-------------------------------------------------------------------
----------------- SMBSITE_MAC_TO_CONTACT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.SMBSITE_MAC_TO_CONTACT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.SMBSITE_MAC_TO_CONTACT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:mac::string as mac,
      GET_PATH($1, 'contact_id:$oid')::string as contact_id,
      GET_PATH($1, 'location_id:$oid')::string as location_id,
      GET_PATH($1, 'account_id:$oid')::string as account_id,
      GET_PATH($1, 'last_seen:$date')::timestamp as last_seen,
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_mac_to_contact.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSTAG.CRM.SMBSITE_MAC_TO_CONTACT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.SMBSITE_MAC_TO_CONTACT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.SMBSITE_MAC_TO_CONTACT_TASK resume;
