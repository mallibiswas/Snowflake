-------------------------------------------------------------------
----------------- ADS_CAMPAIGN_DAILY_SPEND table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.ADS_CAMPAIGN_DAILY_SPEND_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.ADS_CAMPAIGN_DAILY_SPEND as
          select
            $1 as campaign_id,
            $2 as platform_campaign_id,
            $3::date as date,
            $4::number as ads_spend_cents,
            $5::number ads_spend_cents_at_bill_time,
            $6::float as margin,
            $7::timestamp as created,
            $8::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/ads_campaign_daily_spend.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.ADS_CAMPAIGN_DAILY_SPEND_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.ADS_CAMPAIGN_DAILY_SPEND_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.ADS_CAMPAIGN_DAILY_SPEND_TASK resume;