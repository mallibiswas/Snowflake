-------------------------------------------------------------------
----------------- SMBSITE_OFFERLOG_DETAIL table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_OFFERLOG_DETAIL_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_OFFERLOG_DETAIL as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:is_error::boolean as is_error,
      $1:offer_log_id::string as offer_log_id,
      $1:sms_blast_id::string as sms_blast_id,
      GET_PATH($1, 'timestamp:$date')::timestamp as timestamp,
      $1:event::string as event,
      $1:url::string as url,
      $1:response::string as response,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_offerlogdetail.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_OFFERLOG_DETAIL_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_OFFERLOG_DETAIL_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_OFFERLOG_DETAIL_TASK resume;
