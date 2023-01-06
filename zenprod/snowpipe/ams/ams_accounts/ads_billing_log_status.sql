-------------------------------------------------------------------
----------------- ADS_BILLING_LOG_STATUS table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_STATUS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_STATUS as
          select
            $1 as ads_billing_log_id,
            $2 as status,
            $3::timestamp as updated,
            $4 as note,
            $5 as username,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/ads_billing_log_status.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_STATUS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_STATUS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.ADS_BILLING_LOG_STATUS_TASK resume;