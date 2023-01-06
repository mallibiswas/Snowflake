-------------------------------------------------------------------
----------------- ADS_BILLING_LOG_SPEND table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_SPEND_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_SPEND as
          select
            $1 ads_billing_log_id,
            $2::number as spend_cents,
            $3::timestamp as date,
            $4::float as margin,
            $5 as paltform_campaign_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/ads_billing_log_spend.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_SPEND_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_SPEND_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.ADS_BILLING_LOG_SPEND_TASK resume;