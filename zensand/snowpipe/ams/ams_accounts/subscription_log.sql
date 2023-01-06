-------------------------------------------------------------------
----------------- SUBSCRIPTION_LOG table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_LOG_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_LOG as
          select
            $1 as id,
            $2 as account_id,
            $3 as salesforce_quote_line_item_uuid,
            $4 as subscription_id,
            $5::boolean as active,
            $6 as product,
            $7 as package,
            $8::boolean as manual_invoice,
            $9::number as unit_price_cents,
            $10::number as quantity,
            $11::timestamp as start_date,
            $12::number as billing_frequency_months,
            $13 as notes,
            $14 as operation,
            $15 as error,
            $16::timestamp as created,
            $17::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/subscription_log.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_LOG_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_LOG_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_ACCOUNTS.SUBSCRIPTION_LOG_TASK resume;