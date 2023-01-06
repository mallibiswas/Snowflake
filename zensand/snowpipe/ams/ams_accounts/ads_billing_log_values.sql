-------------------------------------------------------------------
----------------- ADS_BILLING_LOG_VALUES table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_VALUES_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_VALUES as
          select
            $1 as status,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/ads_billing_log_values.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_VALUES_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_VALUES_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_VALUES_TASK resume;