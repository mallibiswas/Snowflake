-------------------------------------------------------------------
----------------- ANALYTICS_CUSTOMER table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.ANALYTICS_CUSTOMER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.ANALYTICS_CUSTOMER as
    select  
      GET_PATH($1, '_id:$oid') as id,
      $1:messages_sent::integer as messages_sent,
      $1:birthday_day::integer as birthday_day,
      GET_PATH($1, 'first_seen:$date')::datetime as first_seen,
      $1:visit_count::integer as visit_count,
      $1:non_customer::boolean as non_customer,
      $1:city::string as city,
      DATEADD(day,-1,current_date()) as asof_date
       FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_customer.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.ANALYTICS_CUSTOMER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.ANALYTICS_CUSTOMER_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.ANALYTICS_CUSTOMER_TASK resume;
