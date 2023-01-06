-------------------------------------------------------------------
----------------- REPMANAGEMENT_BUSINESSRATING table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.REPMANAGEMENT_BUSINESSRATING_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.REPMANAGEMENT_BUSINESSRATING as
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
      current_timestamp() as asof_date
    FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/repmanagement_businessrating.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.REPMANAGEMENT_BUSINESSRATING_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.REPMANAGEMENT_BUSINESSRATING_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.REPMANAGEMENT_BUSINESSRATING_TASK resume;
