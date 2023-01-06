-------------------------------------------------------------------
----------------- SMBSITE_EMAILBLAST table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.SMBSITE_EMAILBLAST_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.SMBSITE_EMAILBLAST as
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
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_emailblast.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSTAG.CRM.SMBSITE_EMAILBLAST_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.SMBSITE_EMAILBLAST_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.SMBSITE_EMAILBLAST_TASK resume;
