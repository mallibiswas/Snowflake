-------------------------------------------------------------------
----------------- REPMANAGEMENT_BUSINESSRATING table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.REPMANAGEMENT_BUSINESSRATING_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.REPMANAGEMENT_BUSINESSRATING as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:rating::integer as rating,
      GET_PATH($1, 'updated:$date')::timestamp as updated,
      GET_PATH($1, 'user_id:$oid')::string as user_id,
      GET_PATH($1, 'created:$date')::timestamp as created,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:comments::string as comments,
      GET_PATH($1, 'comments_updated:$date')::timestamp as comments_updated,
      GET_PATH($1, 'rating_updated:$date')::timestamp as rating_updated,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/repmanagement_businessrating.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.REPMANAGEMENT_BUSINESSRATING_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.REPMANAGEMENT_BUSINESSRATING_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.REPMANAGEMENT_BUSINESSRATING_TASK resume;
