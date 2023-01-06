-------------------------------------------------------------------
----------------- ADS_BILLING_LOG_CREDIT_APPLIED_CENTS table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_APPLIED_CENTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_APPLIED_CENTS as
          select
            $1 as ads_billing_log_id,
            $2 as credit_id,
            $3::number as cents,
            $4::number as total_credit_cents,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/ads_billing_log_credit_applied_cents.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_APPLIED_CENTS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_APPLIED_CENTS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_CREDIT_APPLIED_CENTS_TASK resume;