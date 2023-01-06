-------------------------------------------------------------------
----------------- CAMPAIGN table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.CAMPAIGN_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.CAMPAIGN as
          select
            $1 as id,
            $2 as account_id,
            $3::date as start_date,
            $4::date as end_date,
            $5::number as total_price_cents,
            $6::number as margin_percent,
            $7::boolean as manual_invoice,
            $8::boolean as overspend,
            $9::boolean as dirty,
            $10::timestamp as created,
            $11::timestamp as updated,
            $12 as internal_description,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/campaign.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.CAMPAIGN_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.CAMPAIGN_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.CAMPAIGN_TASK resume;