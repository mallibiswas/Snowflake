-------------------------------------------------------------------
----------------- SMBSITE_OFFERLOG_DETAIL table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_OFFERLOG_DETAIL_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_OFFERLOG_DETAIL as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:is_error::boolean as is_error,
      $1:offer_log_id::string as offer_log_id,
      $1:sms_blast_id::string as sms_blast_id,
      GET_PATH($1, 'timestamp:$date')::timestamp as timestamp,
      $1:event::string as event,
      $1:url::string as url,
      $1:response::string as response,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_offerlogdetail.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.SMBSITE_OFFERLOG_DETAIL_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_OFFERLOG_DETAIL_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_OFFERLOG_DETAIL_TASK resume;
