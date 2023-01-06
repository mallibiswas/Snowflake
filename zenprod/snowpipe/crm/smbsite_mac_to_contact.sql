-------------------------------------------------------------------
----------------- SMBSITE_MAC_TO_CONTACT table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_MAC_TO_CONTACT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_MAC_TO_CONTACT as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:mac::string as mac,
      GET_PATH($1, 'contact_id:$oid')::string as contact_id,
      GET_PATH($1, 'location_id:$oid')::string as location_id,
      GET_PATH($1, 'account_id:$oid')::string as account_id,
      GET_PATH($1, 'last_seen:$date')::timestamp as last_seen,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_mac_to_contact.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_MAC_TO_CONTACT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_MAC_TO_CONTACT_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_MAC_TO_CONTACT_TASK resume;
