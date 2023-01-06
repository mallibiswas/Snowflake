-------------------------------------------------------------------
----------------- SMBSITE_EMAILBLAST table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_EMAILBLAST_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_EMAILBLAST as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'scheduled:$date')::timestamp as scheduled,
      $1:repeat_customers::integer as repeat_customers,
      $1:lost_customers::integer as lost_customers,
      $1:new_customers::integer as new_customers,
      $1:loyal_customers::integer as loyal_customers,
      $1:employee_filtered_count::integer as employee_filtered_count,
      $1:queued_count::integer as queued_count,
      $1:sent_count::integer as sent_count,
      GET_PATH($1, 'created:$date')::timestamp as created,
      GET_PATH($1, 'target_id:$oid')::string as target_id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:unsubscribed_filtered_count::integer as unsubscribed_filtered_count,
      $1:target_size::integer as target_size,
      $1:invalid_filtered_count::integer as invalid_filtered_count,
      $1:bounced_filtered_count::integer as bounced_filtered_count,
      GET_PATH($1, 'message_id:$oid')::string as message_id,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_emailblast.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.SMBSITE_EMAILBLAST_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_EMAILBLAST_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_EMAILBLAST_TASK resume;
