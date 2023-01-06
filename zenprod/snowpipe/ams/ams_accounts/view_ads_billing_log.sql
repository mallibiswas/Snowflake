-------------------------------------------------------------------
----------------- VIEW_ADS_BILLING_LOG table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.VIEW_ADS_BILLING_LOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.VIEW_ADS_BILLING_LOG as
          select
            $1 as id,
            $2 as campaign_id,
            $3::timestamp as start_date,
            $4::timestamp as end_date,
            $5::number as total_ads_spend,
            $6::number as total_billed_cents,
            $7 as charge_id,
            $8 as error,
            $9::timestamp as created,
            $10::timestamp as updated,
            $11::number as total_spend_with_margin_cents,
            $12::number as total_spend_before_cap_cents,
            $13::number as previous_billed_cents,
            $14::number as io_budget_cents,
            $15::number as billing_month,
            $16 as status,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/view_ads_billing_log.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.VIEW_ADS_BILLING_LOG_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.VIEW_ADS_BILLING_LOG_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.VIEW_ADS_BILLING_LOG_TASK resume;