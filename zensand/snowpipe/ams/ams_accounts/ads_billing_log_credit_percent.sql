-------------------------------------------------------------------
----------------- ADS_BILLING_LOG_CREDIT_PERCENT table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_PERCENT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_PERCENT as
          select
            $1 as ads_billing_log_id,
            $2::number as credit_id,
            $3::number as value,
            $4::number as cents,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/ads_billing_log_credit_percent.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_PERCENT_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_PERCENT_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_PERCENT_TASK resume;