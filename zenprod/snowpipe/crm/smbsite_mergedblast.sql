-------------------------------------------------------------------
----------------- SMBSITE_MERGEDBLAST table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.SMBSITE_MERGEDBLAST_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.SMBSITE_MERGEDBLAST as
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
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_mergedblast.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.SMBSITE_MERGEDBLAST_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.SMBSITE_MERGEDBLAST_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.SMBSITE_MERGEDBLAST_TASK resume;
