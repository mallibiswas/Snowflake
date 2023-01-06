-------------------------------------------------------------------
----------------- ANALYTICS_COLLECTIONSTATS table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.ANALYTICS_COLLECTIONSTATS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.ANALYTICS_COLLECTIONSTATS as
    select  
      GET_PATH($1, '_id:$oid') as id,
      $1:corrected_emails::integer as corrected_emails,
      $1:follows::integer as follows,
      $1:valid_emails::integer as valid_emails,
      $1:mailgun_corrected_emails::integer as mailgun_corrected_emails,
      $1:wifast_corrected_emails::integer as wifast_corrected_emails,
      $1:likes::integer as likes,
      $1:phones::integer as phones,
      $1:invalid_emails::integer as invalid_emails,
      $1:emails::integer as emails,
      DATEADD(day,-1,current_date()) as asof_date
       FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_collectionstats.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.ANALYTICS_COLLECTIONSTATS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.ANALYTICS_COLLECTIONSTATS_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.ANALYTICS_COLLECTIONSTATS_TASK resume;
