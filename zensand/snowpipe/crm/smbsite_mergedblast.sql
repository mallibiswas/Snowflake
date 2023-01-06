-------------------------------------------------------------------
----------------- SMBSITE_MERGEDBLAST table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_MERGEDBLAST_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_MERGEDBLAST as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'scheduled:$date')::timestamp as scheduled,
      $1:target::variant as target,
      GET_PATH($1, 'created:$date')::timestamp as created,
      GET_PATH($1, 'sort_key:$numberLong')::integer as sort_key,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:sms_blast_id::string as sms_blast_id,
      $1:draft::boolean as draft,
      GET_PATH($1, 'email_blast_id:$oid')::string as email_blast_id,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_mergedblast.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_MERGEDBLAST_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_MERGEDBLAST_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_MERGEDBLAST_TASK resume;
