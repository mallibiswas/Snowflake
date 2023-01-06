-------------------------------------------------------------------
----------------- RECURLY_SUBSCRIPTION table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_ACCOUNTS.RECURLY_SUBSCRIPTION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_ACCOUNTS.RECURLY_SUBSCRIPTION as
          select
            $1 as id,
            $2 as recurly_subscription_id,
            $3::timestamp as start_date,
            $4::number as unit_price_cents,
            $5::boolean as active,
            $6::number as quantity,
            $7 as collection_method,
            $8 as notes,
            $9 as url,
            $10 as plan_code,
            $11::number as billing_frequency_months,
            $12::timestamp as created,
            $13::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/recurly_subscription.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_ACCOUNTS.RECURLY_SUBSCRIPTION_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_ACCOUNTS.RECURLY_SUBSCRIPTION_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_ACCOUNTS.RECURLY_SUBSCRIPTION_TASK resume;