-------------------------------------------------------------------
----------------- SALESFORCE_ORDER table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER as
          select
            $1 as id,
            $2 as status,
            $3::timestamp as effective_date,
            $4::timestamp as created,
            $5::timestamp as updated,
            $6::timestamp as signed_date,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/salesforce_order.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ACCOUNTS.SALESFORCE_ORDER_TASK resume;