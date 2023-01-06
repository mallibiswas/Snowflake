-------------------------------------------------------------------
----------------- ANALYTICS_CUSTOMER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.CRM.ANALYTICS_CUSTOMER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSTAG.CRM.ANALYTICS_CUSTOMER as
    select  
      GET_PATH($1, '_id:$oid') as id,
      $1:messages_sent::integer as messages_sent,
      $1:birthday_day::integer as birthday_day,
      GET_PATH($1, 'first_seen:$date')::datetime as first_seen,
      $1:visit_count::integer as visit_count,
      $1:non_customer::boolean as non_customer,
      $1:city::string as city,
      current_timestamp() as asof_date
       FROM @ZENSTAG.CRM.MONGO_S3_STAGE/${FILE_DATE}/analytics_customer.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.CRM.ANALYTICS_CUSTOMER_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.CRM.ANALYTICS_CUSTOMER_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.CRM.ANALYTICS_CUSTOMER_TASK resume;
